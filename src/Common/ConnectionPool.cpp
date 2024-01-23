#include <Common/ConnectionPool.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/ProxyConfiguration.h>

#include <Poco/Net/HTTPClientSession.h>

#include "config.h"

#if USE_SSL
#include <Poco/Net/HTTPSClientSession.h>
#endif


namespace ProfileEvents
{
    extern const Event S3ConnectionsCreated;
    extern const Event S3ConnectionsReused;
    extern const Event S3ConnectionsReset;
    extern const Event S3ConnectionsPreserved;
    extern const Event S3ConnectionsExpired;
    extern const Event S3ConnectionsErrors;
    extern const Event S3ConnectionsElapsedMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric S3ConnectionsInPool;
    extern const Metric S3ConnectionsActive;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


Poco::Net::HTTPClientSession::ProxyConfig proxyConfigurationToPocoProxyConfig(const DB::ProxyConfiguration & proxy_configuration)
{
    Poco::Net::HTTPClientSession::ProxyConfig poco_proxy_config;

    poco_proxy_config.host = proxy_configuration.host;
    poco_proxy_config.port = proxy_configuration.port;
    poco_proxy_config.protocol = DB::ProxyConfiguration::protocolToString(proxy_configuration.protocol);
    poco_proxy_config.tunnel = proxy_configuration.tunneling;
    poco_proxy_config.originalRequestProtocol = DB::ProxyConfiguration::protocolToString(proxy_configuration.original_request_protocol);

    return poco_proxy_config;
}

Poco::Timespan divide(const Poco::Timespan span, int divisor)
{
    return Poco::Timespan(Poco::Timestamp::TimeDiff(span.totalMicroseconds() / divisor));
}


template <class Session>
DB::ConnectionPool<Session>::ConnectionPool(
    String host_, UInt16 port_, bool https_,
    DB::ProxyConfiguration proxy_configuration_,
    size_t soft_limit_, size_t warn_limit_)
    : host(std::move(host_))
    , port(port_)
    , https(https_)
    , proxy_configuration(std::move(proxy_configuration_))
    , soft_limit(soft_limit_)
    , warn_limit(warn_limit_)
    , resolve_pool(HostResolvePool::create(host))
{}

template <class Session>
DB::ConnectionPool<Session>::WeakPtr DB::ConnectionPool<Session>::getWeakFromThis()
{
    return DB::ConnectionPool<Session>::weak_from_this();
}

template <class Session>
DB::ConnectionPool<Session>::ConnectionPool::~ConnectionPool()
{
    std::lock_guard lock(mutex);
    CurrentMetrics::sub(CurrentMetrics::S3ConnectionsInPool, stored_connections.size());
}

template <class Session>
void DB::ConnectionPool<Session>::PooledConnection::reconnect()
{
    ProfileEvents::increment(ProfileEvents::S3ConnectionsReset);
    Session::close();

    if (auto lock = pool.lock())
    {
        auto new_session = lock->getConnection();
        jumpToOtherConnection(*new_session);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::S3ConnectionsCreated);
        Session::reconnect();
    }
}

template <class Session>
DB::ConnectionPool<Session>::PooledConnection::~PooledConnection()
{
    if (auto lock = pool.lock())
    {
        --lock->active_connections;
        lock->storeDestroyingConnection(*this);
    }
    CurrentMetrics::sub(CurrentMetrics::S3ConnectionsActive);
}

template <class Session>
template<class... Args>
DB::ConnectionPool<Session>::PooledConnection::PooledConnection(DB::ConnectionPool<Session> & pool_, Args&&... args)
    : DB::ConnectionPool<Session>::PooledConnection::Base(args...)
    , pool(pool_.getWeakFromThis())
{
    ++pool_.active_connections;
    CurrentMetrics::add(CurrentMetrics::S3ConnectionsActive);
}

template <class Session>
template<class... Args>
DB::ConnectionPool<Session>::PooledConnection::Ptr DB::ConnectionPool<Session>::PooledConnection::create(Args&&... args)
{
    struct make_shared_enabler : public PooledConnection
    {
        make_shared_enabler(Args&&... args) : PooledConnection(args...) {}
    };
    return std::make_shared<make_shared_enabler>(args...);
}

template <class Session>
void DB::ConnectionPool<Session>::PooledConnection::jumpToOtherConnection(PooledConnection & connection)
{
    chassert(this != &connection);

    auto buffer = Poco::Buffer<char>(0);
    connection.drainBuffer(buffer);
    /// may be it should be like drop this connection and take another from pool
    if (!buffer.empty())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "jumpToOtherConnection with buffered data in src");
    Session::drainBuffer(buffer);
    if (!buffer.empty())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "jumpToOtherConnection with buffered data in dst");

    if (Session::getHost() != connection.getHost())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "jumpToOtherConnection with different host");

    Session::attachSocket(connection.detachSocket());
    Session::setLastRequest(connection.getLastRequest());
    Session::setResolvedHost(connection.getResolvedHost());
    Session::setKeepAlive(connection.getKeepAlive());

    if (!connection.getProxyConfig().host.empty())
        Session::setProxyConfig(connection.getProxyConfig());
}

template <class Session>
DB::ConnectionPool<Session>::PooledConnection::Ptr DB::ConnectionPool<Session>::getConnection()
{
    size_t reused = 0;
    /// upd stats outsize of critical section
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::S3ConnectionsReused, reused);
        CurrentMetrics::sub(CurrentMetrics::S3ConnectionsInPool, reused);
    });

    wipeExpired();

    {
        std::lock_guard lock(mutex);
        if (!stored_connections.empty())
        {
            auto it = stored_connections.top();
            stored_connections.pop();
            ++reused;
            return it;
        }
    }

    return prepareNewConnection();
}

template <class Session>
void DB::ConnectionPool<Session>::wipeExpired()
{
    size_t expired = 0;
    /// upd stats outsize of critical section
    SCOPE_EXIT({
        CurrentMetrics::sub(CurrentMetrics::S3ConnectionsInPool, expired);
        ProfileEvents::increment(ProfileEvents::S3ConnectionsExpired, expired);
    });

    Poco::Timestamp now;

    {
        std::lock_guard lock(mutex);
        while (!stored_connections.empty())
        {
            auto it = stored_connections.top();
            if (!isExpired(now, it))
            {
                return;
            }
            stored_connections.pop();
            ++expired;
        }
    }
}

size_t roundUp(size_t x, size_t rounding)
{
    return (x + (rounding - 1)) / rounding * rounding;
}

template <class Session>
DB::ConnectionPool<Session>::PooledConnection::Ptr DB::ConnectionPool<Session>::prepareNewConnection()
{
    auto address = resolve_pool->get();

    auto active_sessions_ = active_connections.load();
    auto mute_warn_until_ = mute_warn_until.load();

    if (active_sessions_ >= warn_limit && active_sessions_ >= mute_warn_until_)
    {
        LOG_WARNING(log, "Too many active sessions for the host {}, count {}", host, active_sessions_);
        mute_warn_until.store(roundUp(active_sessions_, 100));
    }

    if (active_sessions_ < warn_limit && mute_warn_until_ > 0)
    {
        LOG_WARNING(log, "Sessions count is OK for the host {}, count {}", host, active_sessions_);
        mute_warn_until.store(0);
    }

    auto session = PooledConnection::create(*this, host, port);

    if (proxy_configuration.host.empty())
        session->setProxyConfig(proxyConfigurationToPocoProxyConfig(proxy_configuration));

    session->setResolvedHost(*address);

    session->setKeepAlive(true);

    try {
        auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3ConnectionsElapsedMicroseconds);
        session->doConnect();
    }
    catch (...)
    {
        address.setFail();
        resolve_pool->update();
        ProfileEvents::increment(ProfileEvents::S3ConnectionsErrors);
        throw;
    }

    ProfileEvents::increment(ProfileEvents::S3ConnectionsCreated);
    return session;
}

template <class Session>
void DB::ConnectionPool<Session>::storeDestroyingConnection(PooledConnection & connection)
{
    size_t preserved = 0;

    /// upd stats outsize of critical section
    SCOPE_EXIT({
        CurrentMetrics::add(CurrentMetrics::S3ConnectionsInPool, preserved);
        ProfileEvents::increment(ProfileEvents::S3ConnectionsPreserved, preserved);
    });

    if (connection.connected() && hasReuseTag(connection))
    {
        std::lock_guard lock(mutex);

        auto stored = PooledConnection::create(*this, host, port);
        stored->jumpToOtherConnection(connection);

        stored_connections.push(stored);
        ++preserved;
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::S3ConnectionsReset);
    }
}

template <class Session>
bool DB::ConnectionPool<Session>::isExpired(Poco::Timestamp & now, PooledConnection::Ptr connection)
{
    if (stored_connections.size() > soft_limit)
        return now > (connection->getLastRequest() + divide(connection->getKeepAliveTimeout(), 10));
    return now > connection->getLastRequest() + connection->getKeepAliveTimeout();
}

template class DB::ConnectionPool<Poco::Net::HTTPClientSession>;
#if USE_SSL
template class DB::ConnectionPool<Poco::Net::HTTPSClientSession>;
#endif


struct ReuseTag {};

bool DB::hasReuseTag(Poco::Net::HTTPSession & session)
{
    const auto & session_data = session.sessionData();
    return !session_data.empty() && Poco::AnyCast<ReuseTag>(&session_data);
}

void DB::resetReuseTag(Poco::Net::HTTPSession & session)
{
    session.attachSessionData({});
}

void DB::setReuseTag(Poco::Net::HTTPSession & session)
{
    session.attachSessionData(ReuseTag{});
}
