#pragma once

#include <Common/ProxyConfiguration.h>
#include <Common/HostResolvePool.h>
#include <Common/logger_useful.h>

#include <base/defines.h>

#include <Poco/Timespan.h>
#include <Poco/Net/HTTPClientSession.h>

#include <mutex>
#include <memory>

// That class manage connections to the endpoint
// Features:
// - it uses HostResolvePool for address selecting. See Common/HostResolvePool.h for more info.
// - it minimizes number of `Session::connect()`/`Session::reconnect()` calls
//   - stores only connected and ready to use sessions
//   - connection could be reused even when limits are reached
// - soft limit
// - warn limit
// - `Session::reconnect()` uses that pool
// - comprehensive sensors


// connection live stages
// CREATED -> STORED
// CREATED -> RESET
// STORED -> EXPIRED
// STORED -> REUSED
// REUSED -> RESET
// REUSED -> STORED

namespace DB
{

template <class Session>
class ConnectionPool : public std::enable_shared_from_this<ConnectionPool<Session>>
{
private:
    using WeakPtr = std::weak_ptr<ConnectionPool<Session>>;

public:
    using Ptr = std::shared_ptr<ConnectionPool<Session>>;

    template<class... Args>
    static Ptr create(Args&&... args)
    {
        struct make_shared_enabler : public ConnectionPool<Session>
        {
            make_shared_enabler(Args&&... args) : ConnectionPool<Session>(std::forward<Args>(args)...) {}
        };
        return std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
    }

    virtual ~ConnectionPool();

    ConnectionPool(const ConnectionPool &) = delete;
    ConnectionPool & operator=(const ConnectionPool &) = delete;

    class PooledConnection : public std::enable_shared_from_this<PooledConnection>, public Session
    {
    public:
        using Ptr = std::shared_ptr<PooledConnection>;

        void reconnect() override;
        ~PooledConnection() override;

    private:
        friend class ConnectionPool<Session>;

        template<class... Args>
        PooledConnection(ConnectionPool<Session> & pool_, Args&&... args);

        template<class... Args>
        static Ptr create(Args&&... args);

        void doConnect() { Session::reconnect(); }

        void jumpToOtherConnection(PooledConnection & connection);

        typedef Session Base;

        ConnectionPool::WeakPtr pool;
    };

    PooledConnection::Ptr getConnection();

private:
    ConnectionPool(String host_, UInt16 port_, bool https_,
                DB::ProxyConfiguration proxy_configuration_,
                size_t soft_limit_ = 1000,
                size_t warn_limit_ = 20000);

    friend class PooledConnection;
    WeakPtr getWeakFromThis();

    const std::string host;
    const UInt16 port;
    const bool https;
    const DB::ProxyConfiguration proxy_configuration;
    const size_t soft_limit;
    const size_t warn_limit;

    Poco::Logger * log = &Poco::Logger::get("ConnectionPool");

    struct CompareByLastRequest
    {
        static bool operator() (const PooledConnection::Ptr & l, const PooledConnection::Ptr & r)
        {
            return l->getLastRequest() > r->getLastRequest();
        }
    };

    using ConnectionsMaxHeap
        = std::priority_queue<typename PooledConnection::Ptr,
                              std::vector<typename PooledConnection::Ptr>, CompareByLastRequest>;

    HostResolvePool::Ptr resolve_pool;

    std::mutex mutex;
    ConnectionsMaxHeap stored_connections TSA_GUARDED_BY(mutex);
    std::atomic<size_t> active_connections = 0;
    std::atomic<size_t> mute_warn_until = 0;

    bool isExpired(Poco::Timestamp & now, PooledConnection::Ptr connection) TSA_REQUIRES(mutex);

    void wipeExpired();

    PooledConnection::Ptr prepareNewConnection();

    void storeDestroyingConnection(PooledConnection & connection);
};

bool hasReuseTag(Poco::Net::HTTPSession & session);
void resetReuseTag(Poco::Net::HTTPSession & session);
void setReuseTag(Poco::Net::HTTPSession & session);

}
