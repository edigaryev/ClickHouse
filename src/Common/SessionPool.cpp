#include <Common/SessionPool.h>

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
    extern const Event S3SessionCreated;
    extern const Event S3SessionReused;
    extern const Event S3SessionReset;
    extern const Event S3SessionPreserved;
    extern const Event S3SessionExpired;
    extern const Event S3SessionConnectErrors;
    extern const Event S3SessionConnectMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric S3SessionInPool;
    extern const Metric S3SessionActive;
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
DB::SessionPool<Session>::SessionPool(
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
DB::SessionPool<Session>::WeakPtr DB::SessionPool<Session>::getWeakFromThis()
{
    return DB::SessionPool<Session>::weak_from_this();
}

template <class Session>
DB::SessionPool<Session>::SessionPool::~SessionPool()
{
    std::lock_guard lock(mutex);
    CurrentMetrics::sub(CurrentMetrics::S3SessionInPool, stored_sessions.size());
}

template <class Session>
void DB::SessionPool<Session>::PooledSession::reconnect()
{
    ProfileEvents::increment(ProfileEvents::S3SessionReset);
    Session::close();

    if (auto lock = pool.lock())
    {
        auto new_session = lock->getSession();
        jumpToSession(*new_session);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::S3SessionCreated);
        Session::reconnect();
    }
}

template <class Session>
DB::SessionPool<Session>::PooledSession::~PooledSession()
{
    if (auto lock = pool.lock())
    {
        --lock->active_sessions;
        lock->storeDestroyingSession(*this);
    }
    CurrentMetrics::sub(CurrentMetrics::S3SessionActive);
}

template <class Session>
template<class... Args>
DB::SessionPool<Session>::PooledSession::PooledSession(DB::SessionPool<Session> & pool_, Args&&... args)
    : DB::SessionPool<Session>::PooledSession::Base(args...)
    , pool(pool_.getWeakFromThis())
{
    ++pool_.active_sessions;
    CurrentMetrics::add(CurrentMetrics::S3SessionActive);
}

template <class Session>
template<class... Args>
DB::SessionPool<Session>::PooledSession::Ptr DB::SessionPool<Session>::PooledSession::create(Args&&... args)
{
    struct make_shared_enabler : public PooledSession
    {
        make_shared_enabler(Args&&... args) : PooledSession(args...) {}
    };
    return std::make_shared<make_shared_enabler>(args...);
}

template <class Session>
void DB::SessionPool<Session>::PooledSession::jumpToSession(DB::SessionPool<Session>::PooledSession & session)
{
    auto buffer = Poco::Buffer<char>(0);
    session.drainBuffer(buffer);
    /// may be it should be like drop this connection and take another from pool
    if (!buffer.empty())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "jumpToSession with buffered data in src");
    Session::drainBuffer(buffer);
    if (!buffer.empty())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "jumpToSession with buffered data in dst");

    if (Session::getHost() != session.getHost())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "jumpToSession with different host");

    Session::attachSocket(session.detachSocket());
    Session::setLastRequest(session.getLastRequest());
    Session::setResolvedHost(session.getResolvedHost());
    Session::setKeepAlive(session.getKeepAlive());

    if (!session.getProxyConfig().host.empty())
        Session::setProxyConfig(session.getProxyConfig());
}

template <class Session>
DB::SessionPool<Session>::PooledSession::Ptr DB::SessionPool<Session>::getSession()
{
    size_t reused = 0;
    /// upd stats outsize of critical section
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::S3SessionReused, reused);
        CurrentMetrics::sub(CurrentMetrics::S3SessionInPool, reused);
    });

    wipeExpired();

    {
        std::lock_guard lock(mutex);
        if (!stored_sessions.empty())
        {
            auto it = stored_sessions.top();
            stored_sessions.pop();
            ++reused;
            return it;
        }
    }

    return prepareNewSession();
}

template <class Session>
void DB::SessionPool<Session>::wipeExpired()
{
    size_t expired = 0;
    /// upd stats outsize of critical section
    SCOPE_EXIT({
        CurrentMetrics::sub(CurrentMetrics::S3SessionInPool, expired);
        ProfileEvents::increment(ProfileEvents::S3SessionExpired, expired);
    });

    Poco::Timestamp now;

    {
        std::lock_guard lock(mutex);
        while (!stored_sessions.empty())
        {
            auto it = stored_sessions.top();
            if (!isExpired(now, it))
            {
                return;
            }
            stored_sessions.pop();
            ++expired;
        }
    }
}

size_t roundUp(size_t x, size_t rounding)
{
    return (x + (rounding - 1)) / rounding * rounding;
}

template <class Session>
DB::SessionPool<Session>::PooledSession::Ptr DB::SessionPool<Session>::prepareNewSession()
{
    auto address = resolve_pool->get();

    auto active_sessions_ = active_sessions.load();
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

    auto session = PooledSession::create(*this, host, port);

    if (proxy_configuration.host.empty())
        session->setProxyConfig(proxyConfigurationToPocoProxyConfig(proxy_configuration));

    session->setResolvedHost(*address);

    session->setKeepAlive(true);

    try {
        auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3SessionConnectMicroseconds);
        session->doConnect();
    }
    catch (...)
    {
        address.setFail();
        resolve_pool->update();
        ProfileEvents::increment(ProfileEvents::S3SessionConnectErrors);
        throw;
    }

    ProfileEvents::increment(ProfileEvents::S3SessionCreated);
    return session;
}

template <class Session>
void DB::SessionPool<Session>::storeDestroyingSession(PooledSession & session)
{
    size_t preserved = 0;

    /// upd stats outsize of critical section
    SCOPE_EXIT({
        CurrentMetrics::add(CurrentMetrics::S3SessionInPool, preserved);
        ProfileEvents::increment(ProfileEvents::S3SessionPreserved, preserved);
    });

    if (session.connected() && hasReuseTag(session))
    {
        std::lock_guard lock(mutex);

        auto stored = PooledSession::create(*this, host, port);
        stored->jumpToSession(session);

        stored_sessions.push(stored);
        ++preserved;
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::S3SessionReset);
    }
}

template <class Session>
bool DB::SessionPool<Session>::isExpired(Poco::Timestamp & now, PooledSession::Ptr session)
{
    if (stored_sessions.size() > soft_limit)
        return now > (session->getLastRequest() + divide(session->getKeepAliveTimeout(), 10));
    return now > session->getLastRequest() + session->getKeepAliveTimeout();
}

template class DB::SessionPool<Poco::Net::HTTPClientSession>;
#if USE_SSL
template class DB::SessionPool<Poco::Net::HTTPSClientSession>;
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
