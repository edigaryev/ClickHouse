#include <Common/HostResolvePool.h>

#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/thread_local_rng.h>

#include <mutex>

namespace ProfileEvents
{
    extern const Event S3IPsNew;
    extern const Event S3IPsExpired;
    extern const Event S3IPsFailScored;
}

namespace CurrentMetrics
{
    extern const Metric S3IPsActive;
}

namespace DB::ErrorCodes
{
    extern const int DNS_ERROR;
}

DB::HostResolvePool::WeakPtr DB::HostResolvePool::getWeakFromThis()
{
    return weak_from_this();
}

DB::HostResolvePool::HostResolvePool(String host_, size_t generation_history_)
    : host(std::move(host_))
    , generation_history(generation_history_)
{
    update();
}

DB::HostResolvePool::~HostResolvePool()
{
    std::lock_guard lock(mutex);
    CurrentMetrics::sub(CurrentMetrics::S3IPsActive, records.size());
}

void DB::HostResolvePool::Entry::setFail()
{
    if (!fail)
    {
        if (auto lock = pool.lock())
        {
            lock->setFail(address);
        }
    }

    fail = true;
}

DB::HostResolvePool::Entry::~Entry()
{
    if (!fail)
    {
        if (auto lock = pool.lock())
        {
            lock->setSuccess(address);
        }
    }
}

void DB::HostResolvePool::update()
{
    auto next_gen = DB::DNSResolver::instance().resolveHostAll(host);
    if (next_gen.empty())
        throw DB::Exception(ErrorCodes::DNS_ERROR, "no endpoints resolved for host {}", host);

    UpdateStats stats;

    /// upd stats outsize of critical section
    SCOPE_EXIT({
        CurrentMetrics::add(CurrentMetrics::S3IPsActive, stats.added);
        CurrentMetrics::sub(CurrentMetrics::S3IPsActive, stats.expired);
        ProfileEvents::increment(ProfileEvents::S3IPsExpired, stats.expired);
    });

    std::lock_guard lock(mutex);
    stats = updateImpl(next_gen);
    initWeightMap();
}

DB::HostResolvePool::Entry DB::HostResolvePool::get()
{
    std::lock_guard lock(mutex);
    return Entry(*this, selectBest());
}

void DB::HostResolvePool::setSuccess(const Poco::Net::IPAddress & address)
{
    std::lock_guard lock(mutex);

    auto it = find(address);
    if (it == records.end())
        return;
    ++it->ussage;
    // cal initWeightMap only when records are updated or setFail
}

void DB::HostResolvePool::setFail(const Poco::Net::IPAddress & address)
{
    std::lock_guard lock(mutex);

    auto it = find(address);
    if (it == records.end())
        return;
    if (it->fail_bit)
        return;
    it->fail_bit = true;
    it->fail_generation = last_generation;
    ProfileEvents::increment(ProfileEvents::S3IPsFailScored);

    initWeightMap();
}

Poco::Net::IPAddress DB::HostResolvePool::selectBest()
{
    chassert(!records.empty());
    size_t weight = random_weight_picker(thread_local_rng);
    auto it = std::lower_bound(
        weight_map.begin(), weight_map.end(),
        weight,
        [](const std::pair<uint8_t, size_t> & rec, size_t value)
        {
            return rec.first < value;
        });
    chassert(it != weight_map.end());
    return records[it->second].address;
}

DB::HostResolvePool::Records::iterator DB::HostResolvePool::find(const Poco::Net::IPAddress & addr) TSA_REQUIRES(mutex)
{
    return std::lower_bound(
        records.begin(), records.end(),
        addr,
        [](const Record& rec, const Poco::Net::IPAddress & value)
        {
            return rec.address < value;
        });
}

DB::HostResolvePool::UpdateStats DB::HostResolvePool::updateImpl(std::vector<Poco::Net::IPAddress> & next_gen) TSA_REQUIRES(mutex)
{
    ++last_generation;
    size_t effective_generation = last_generation > generation_history ? last_generation - generation_history : 0;

    ProfileEvents::increment(ProfileEvents::S3IPsNew, next_gen.size());

    UpdateStats stats;

    for (auto & addr : next_gen)
    {
        auto it = find(addr);

        if (it == records.end() || it->address != addr)
        {
            ++stats.added;
            records.insert(it, Record(addr, last_generation));
        }
        else
        {
            ++stats.updated;

            it->generation = last_generation;
            if (it->fail_bit && it->fail_generation < effective_generation)
            {
                it->fail_bit = false;
            }
        }
    }

    size_t removed = std::erase_if(
        records,
        [=](const Record & rec)
        {
            return rec.fail_bit && rec.generation < effective_generation;
        });

    stats.expired = removed;

    return stats;
}

void DB::HostResolvePool::initWeightMapImpl()
{
    total_weight = 0;
    weight_map.clear();
    for (size_t i = 0; i < records.size(); ++i)
    {
        auto & rec = records[i];
        if (rec.fail_bit)
            continue;
        total_weight += rec.getWeight();
        weight_map.push_back(std::make_pair(total_weight, i));
    }
}

void DB::HostResolvePool::initWeightMap()
{
    initWeightMapImpl();

    if (total_weight == 0 && !records.empty())
    {
        for (auto & rec: records)
            {
            rec.fail_bit = false;
            }

        initWeightMapImpl();
    }

    chassert(total_weight > 0 && !weight_map.empty() && !records.empty());
    random_weight_picker = std::uniform_int_distribution<size_t>(0, total_weight);
}
