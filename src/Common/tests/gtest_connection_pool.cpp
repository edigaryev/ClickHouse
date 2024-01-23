#include <IO/WriteBufferFromFile.h>
#include <Common/ConnectionPool.h>

#include <Poco/URI.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/MessageHeader.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>

#include <thread>
#include <gtest/gtest.h>

size_t stream_copy_n(std::istream & in, std::ostream & out, std::size_t count = std::numeric_limits<size_t>::max())
{
    const size_t buffer_size = 4096;
    char buffer[buffer_size];

    size_t total_read = 0;

    while (count > buffer_size)
    {
        in.read(buffer, buffer_size);
        size_t read = in.gcount();
        out.write(buffer, read);
        count -= read;
        total_read += read;

        if (read == 0)
            return total_read;
    }

    in.read(buffer, count);
    size_t read = in.gcount();
    out.write(buffer, read);
    total_read += read;

    return total_read;
}

class MockRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        size_t size = request.getContentLength();
        response.setContentLength(size); // ContentLength is required for keep alive
        size_t copied = stream_copy_n(request.stream(), response.send(), size);
        std::cerr << "handleRequest: " << copied << "/" << size  << std::endl;
    }
};

class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    virtual Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockRequestHandler();
    }
};

using HTTPSession = Poco::Net::HTTPClientSession;
using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

using SessionPool = DB::ConnectionPool<HTTPSession>;

class ConnectionPoolTest : public testing::Test {
protected:
    ConnectionPoolTest() = default;

    // If the constructor and destructor are not enough for setting up
    // and cleaning up each test, you can define the following methods:

    void SetUp() override {
        DB::CurrentThread::getProfileEvents().reset();
        startServer();
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }

    SessionPool::Ptr makePool()
    {
        auto uri = Poco::URI(getServerUrl());
        std::cerr << "make connection pool to the endpoint: " << uri.toString() << std::endl;
        return SessionPool::create(uri.getHost(), uri.getPort(), uri.getScheme() == "https", DB::ProxyConfiguration());
    }

    std::string getServerUrl()
    {
        return "http://" + server_data.socket->address().toString();
    }

    void startServer()
    {
        server_data = {};
        server_data.params = new Poco::Net::HTTPServerParams();
        server_data.handler_factory = new HTTPRequestHandlerFactory();
        server_data.socket.reset(new Poco::Net::ServerSocket(server_data.port));
        server_data.server.reset(
            new Poco::Net::HTTPServer(server_data.handler_factory, *server_data.socket, server_data.params));

        server_data.server->start();
    }

    Poco::Net::HTTPServer & getServer()
    {
        return *server_data.server;
    }

    struct
    {
        // just some port to avoid collisions with others tests
        UInt16 port = 9871;
        Poco::Net::HTTPServerParams::Ptr params;
        HTTPRequestHandlerFactory::Ptr handler_factory;
        std::unique_ptr<Poco::Net::ServerSocket> socket;
        std::unique_ptr<Poco::Net::HTTPServer> server;
    } server_data;
};

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

void wait_until(std::function<bool()> pred)
{
    while (!pred())
        Poco::Thread::sleep(250);
}

void echoRequest(String data, HTTPSession & session)
{
    {
        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", "HTTP/1.1"); // HTTP/1.1 is required for keep alive
        request.setContentLength(data.size());
        std::ostream & ostream = session.sendRequest(request);
        ostream << data;
    }

    {
        std::stringstream result;
        Poco::Net::HTTPResponse response;
        std::istream & istream = session.receiveResponse(response);
        ASSERT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_OK);

        stream_copy_n(istream, result);
        ASSERT_EQ(data, result.str());
    }
}

TEST_F(ConnectionPoolTest, CanConnect)
{
    auto pool = makePool();
    auto connection = pool->getConnection();

    ASSERT_TRUE(connection->connected());
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsCreated]);

    ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3ConnectionsActive));
    ASSERT_EQ(0, CurrentMetrics::get(CurrentMetrics::S3ConnectionsInPool));

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    connection->reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsCreated]);
}

TEST_F(ConnectionPoolTest, CanRequest)
{
    auto pool = makePool();
    auto connection = pool->getConnection();

    echoRequest("Hello", *connection);

    ASSERT_EQ(1, getServer().totalConnections());
    ASSERT_EQ(1, getServer().currentConnections());

    connection->reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsCreated]);
}

TEST_F(ConnectionPoolTest, CanPreserve)
{
    auto pool = makePool();

    {
        auto connection = pool->getConnection();
        setReuseTag(*connection);
        std::cerr << "implicit save connection with reuse tag" << std::endl;
    }

    ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3ConnectionsActive));
    ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3ConnectionsInPool));

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsCreated]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsPreserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsReused]);
}

TEST_F(ConnectionPoolTest, CanReuse)
{
    auto pool = makePool();

    {
        auto connection = pool->getConnection();
        setReuseTag(*connection);
    }

    ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3ConnectionsActive));
    ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3ConnectionsInPool));

    {
        auto connection = pool->getConnection();

        ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3ConnectionsActive));
        ASSERT_EQ(0, CurrentMetrics::get(CurrentMetrics::S3ConnectionsInPool));

        wait_until([&] () { return getServer().currentConnections() == 1; });
        ASSERT_EQ(1, getServer().currentConnections());

        echoRequest("Hello", *connection);

        ASSERT_EQ(1, getServer().totalConnections());
        ASSERT_EQ(1, getServer().currentConnections());

        std::cerr << "implicit reset connection" << std::endl;
    }

    ASSERT_EQ(0, CurrentMetrics::get(CurrentMetrics::S3ConnectionsActive));
    ASSERT_EQ(0, CurrentMetrics::get(CurrentMetrics::S3ConnectionsInPool));

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsCreated]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsPreserved]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsReused]);
}

TEST_F(ConnectionPoolTest, CanReuse10)
{
    auto pool = makePool();


    for (int i = 0; i < 10; ++i)
    {
        auto connection = pool->getConnection();
        echoRequest("Hello", *connection);
        setReuseTag(*connection);
    }

    {
        [[maybe_unused]] auto tmp = pool->getConnection();
    }

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsCreated]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsPreserved]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsReused]);
}

TEST_F(ConnectionPoolTest, CanReconnectAndCreate)
{
    auto pool = makePool();

    std::vector<HTTPSessionPtr> in_use;

    const size_t count = 2;
    for (int i = 0; i < count; ++i)
    {
        auto connection = pool->getConnection();
        setReuseTag(*connection);
        in_use.push_back(connection);
    }

    ASSERT_EQ(count, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsCreated]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsPreserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsReused]);

    ASSERT_EQ(count, CurrentMetrics::get(CurrentMetrics::S3ConnectionsActive));
    ASSERT_EQ(0, CurrentMetrics::get(CurrentMetrics::S3ConnectionsInPool));

    auto connection = std::move(in_use.back());
    in_use.pop_back();

    echoRequest("Hello", *connection);

    connection->abort(); // further usage requires reconnect, new connection

    echoRequest("Hello", *connection);

    connection->reset();

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());
    ASSERT_EQ(count+1, getServer().totalConnections());

    ASSERT_EQ(count+1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsCreated]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsPreserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsReused]);
}

TEST_F(ConnectionPoolTest, CanReconnectAndReuse)
{
    auto pool = makePool();

    std::vector<HTTPSessionPtr> in_use;

    const size_t count = 2;
    for (int i = 0; i < count; ++i)
    {
        auto connection = pool->getConnection();
        setReuseTag(*connection);
        in_use.push_back(std::move(connection));
    }

    auto connection = std::move(in_use.back());
    in_use.pop_back();
    in_use.clear(); // other connection will be reused

    echoRequest("Hello", *connection);

    connection->abort(); // further usage requires reconnect, reuse connection from pool

    echoRequest("Hello", *connection);

    connection->reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(2, getServer().totalConnections());

    ASSERT_EQ(count, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsCreated]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsPreserved]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3ConnectionsReused]);
}
