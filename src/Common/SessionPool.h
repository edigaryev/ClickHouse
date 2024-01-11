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
//   - session could be reused even when limits are reached
// - soft limit
// - warn limit
// - `Session::reconnect()` uses that pool
// - comprehensive sensors


// session live stages
// CREATED -> STORED
// CREATED -> RESET
// STORED -> EXPIRED
// STORED -> REUSED
// REUSED -> RESET
// REUSED -> STORED

namespace DB
{

template <class Session>
class SessionPool : public std::enable_shared_from_this<SessionPool<Session>>
{
private:
    using WeakPtr = std::weak_ptr<SessionPool<Session>>;

public:
    using Ptr = std::shared_ptr<SessionPool<Session>>;

    template<class... Args>
    static Ptr create(Args&&... args)
    {
        struct make_shared_enabler : public SessionPool<Session>
        {
            make_shared_enabler(Args&&... args) : SessionPool<Session>(std::forward<Args>(args)...) {}
        };
        return std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
    }

    virtual ~SessionPool();

    SessionPool(const SessionPool&) = delete;
    SessionPool& operator=(const SessionPool&) = delete;

    class PooledSession : public std::enable_shared_from_this<PooledSession>, public Session
    {
    public:
        using Ptr = std::shared_ptr<PooledSession>;

        void reconnect() override;
        ~PooledSession() override;

    private:
        friend class SessionPool<Session>;

        template<class... Args>
        PooledSession(SessionPool<Session> & pool_, Args&&... args);

        template<class... Args>
        static Ptr create(Args&&... args);

        void doConnect() { Session::reconnect(); }

        void jumpToSession(PooledSession & session);

        typedef Session Base;

        SessionPool::WeakPtr pool;
    };

    PooledSession::Ptr getSession();

private:
    SessionPool(String host_, UInt16 port_, bool https_,
                DB::ProxyConfiguration proxy_configuration_,
                size_t soft_limit_ = 1000,
                size_t warn_limit_ = 20000);

    friend class PooledSession;
    WeakPtr getWeakFromThis();

    const std::string host;
    const UInt16 port;
    const bool https;
    const DB::ProxyConfiguration proxy_configuration;
    const size_t soft_limit;
    const size_t warn_limit;

    Poco::Logger * log = &Poco::Logger::get("SessionPool");

    struct MaxHeapSessionByLastRequestCompare
    {
        static bool operator() (const PooledSession::Ptr & l, const PooledSession::Ptr & r)
        {
            return l->getLastRequest() > r->getLastRequest();
        }
    };

    using MaxHeapSessionByLastRequest = std::priority_queue<typename PooledSession::Ptr,
                                                            std::vector<typename PooledSession::Ptr>,
                                                            MaxHeapSessionByLastRequestCompare>;

    HostResolvePool::Ptr resolve_pool;

    std::mutex mutex;
    MaxHeapSessionByLastRequest stored_sessions TSA_GUARDED_BY(mutex);
    std::atomic<size_t> active_sessions = 0;
    std::atomic<size_t> mute_warn_until = 0;

    bool isExpired(Poco::Timestamp & now, PooledSession::Ptr session) TSA_REQUIRES(mutex);

    void wipeExpired();

    PooledSession::Ptr prepareNewSession();

    void storeDestroyingSession(PooledSession & session);
};

bool hasReuseTag(Poco::Net::HTTPSession & session);
void resetReuseTag(Poco::Net::HTTPSession & session);
void setReuseTag(Poco::Net::HTTPSession & session);

}
