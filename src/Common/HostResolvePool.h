#pragma once

#include <Common/logger_useful.h>
#include <base/defines.h>
#include <Poco/Net/IPAddress.h>

#include <mutex>
#include <memory>

// That class resolves host into multiply addresses
// Features:
// - balance address usage.
//    `selectBest()` chooses the address by random with weights.
//    The more ip is used the lesser weight it has. When new address is happened, it takes more weight.
//    But still not all requests are assigned to the new address.
// - join resolve results
//    In case when host is resolved into different set of addresses, this class join all that addresses and use them.
//    An address expires after several (generation_history_) consecutive resolves without that address.
// - failing address pessimization
//    If an address marked with `setFail()` it is marked as faulty. Such address won't be selected until either
//    a) it still occurs in resolve set after generation_history resolves or b) all other addresses are pessimized as well.

namespace DB
{

class HostResolvePool : public std::enable_shared_from_this<HostResolvePool>
{
private:
    using WeakPtr = std::weak_ptr<HostResolvePool>;

public:
    using Ptr = std::shared_ptr<HostResolvePool>;

    template<class... Args>
    static Ptr create(Args&&... args)
    {
        struct make_shared_enabler : public HostResolvePool
        {
            make_shared_enabler(Args&&... args) : HostResolvePool(std::forward<Args>(args)...) {}
        };
        return std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
    }

    virtual ~HostResolvePool();

    class Entry
    {
    public:
        explicit Entry(Entry && entry) = default;
        explicit Entry(Entry & entry) = delete;

        // no access as r-value
        const String * operator->() && = delete;
        const String * operator->() const && = delete;
        const String & operator*() && = delete;
        const String & operator*() const && = delete;

        const String * operator->() & { return &resolved_host; }
        const String * operator->() const & { return &resolved_host; }
        const String & operator*() & { return resolved_host; }
        const String & operator*() const & { return resolved_host; }

        void setFail();
        ~Entry();

    private:
        friend class HostResolvePool;

        explicit Entry(HostResolvePool & pool_, Poco::Net::IPAddress address_)
            : pool(pool_.getWeakFromThis())
            , address(std::move(address_))
            , resolved_host(address.toString())
        { }

        HostResolvePool::WeakPtr pool;
        const Poco::Net::IPAddress address;
        const String resolved_host;

        bool fail = false;
    };

    Entry get();
    void update();

private:
    explicit HostResolvePool(String host_, size_t generation_history_  = 3);

    friend class Entry;
    WeakPtr getWeakFromThis();

    void setSuccess(const Poco::Net::IPAddress & address);
    void setFail(const Poco::Net::IPAddress & address);

    struct Record
    {
        Record(Poco::Net::IPAddress address_, size_t generation_)
            : address(std::move(address_))
            , generation(generation_)
        {}

        explicit Record(Record && rec) = default;
        Record& operator=(Record && s) = default;

        explicit Record(Record & rec) = delete;
        Record& operator=(Record & s) = delete;

        Poco::Net::IPAddress address;
        size_t generation = 0;
        size_t ussage = 0;
        bool fail_bit = false;
        size_t fail_generation = 0;

        uint8_t getWeight() const
        {
            if (ussage > 10000)
                return 1;
            if (ussage > 1000)
                return 3;
            if (ussage > 100)
                return 5;
            return 10;
        }
    };

    using Records = std::vector<Record>;

    struct UpdateStats
    {
        size_t added = 0;
        size_t updated = 0;
        size_t expired = 0;
    };

    Poco::Net::IPAddress selectBest() TSA_REQUIRES(mutex);
    Records::iterator find(const Poco::Net::IPAddress & address) TSA_REQUIRES(mutex);
    UpdateStats updateImpl(std::vector<Poco::Net::IPAddress> & next_gen) TSA_REQUIRES(mutex);
    void initWeightMapImpl() TSA_REQUIRES(mutex);
    void initWeightMap() TSA_REQUIRES(mutex);

    const String host;
    const size_t generation_history;

    std::mutex mutex;
    Records records TSA_GUARDED_BY(mutex);

    size_t total_weight = 0;
    std::uniform_int_distribution<size_t> random_weight_picker;
    std::vector<std::pair<uint8_t, size_t>> weight_map;

    size_t last_generation = 0;

    Poco::Logger * log = &Poco::Logger::get("SessionPool");
};

}

