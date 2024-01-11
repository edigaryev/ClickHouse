#include <IO/WriteBufferFromFile.h>
#include <Common/SessionPool.h>

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

using SessionPool = DB::SessionPool<HTTPSession>;

class SessionPoolTest : public testing::Test {
protected:
    SessionPoolTest() = default;

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
        server_data.socket.reset(new Poco::Net::ServerSocket(0));
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
        Poco::Net::HTTPServerParams::Ptr params;
        HTTPRequestHandlerFactory::Ptr handler_factory;
        std::unique_ptr<Poco::Net::ServerSocket> socket;
        std::unique_ptr<Poco::Net::HTTPServer> server;
    } server_data;
};

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

TEST_F(SessionPoolTest, CanConnect)
{
    std::cerr << "create pool" << std::endl;
    auto pool = makePool();
    std::cerr << "create session" << std::endl;
    auto session = pool->getSession();

    ASSERT_TRUE(session->connected());
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionCreated]);

    ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3SessionActive));
    ASSERT_EQ(0, CurrentMetrics::get(CurrentMetrics::S3SessionInPool));

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    std::cerr << "reset session" << std::endl;
    session->reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionCreated]);
}

TEST_F(SessionPoolTest, CanRequest)
{
    auto pool = makePool();
    auto session = pool->getSession();

    std::cerr << "make request" << std::endl;
    echoRequest("Hello", *session);

    ASSERT_EQ(1, getServer().totalConnections());
    ASSERT_EQ(1, getServer().currentConnections());

    std::cerr << "reset session" << std::endl;
    session->reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionCreated]);
}

TEST_F(SessionPoolTest, CanPreserve)
{
    auto pool = makePool();

    {
        std::cerr << "create session" << std::endl;
        auto session = pool->getSession();
        std::cerr << "set reuse tag" << std::endl;
        setReuseTag(*session);
        std::cerr << "implicit save session with reuse tag" << std::endl;
    }

    ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3SessionActive));
    ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3SessionInPool));

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionCreated]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionPreserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionReused]);
}

TEST_F(SessionPoolTest, CanReuse)
{
    auto pool = makePool();

    {
        auto session = pool->getSession();
        setReuseTag(*session);
    }

    ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3SessionActive));
    ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3SessionInPool));

    {
        auto session = pool->getSession();

        ASSERT_EQ(1, CurrentMetrics::get(CurrentMetrics::S3SessionActive));
        ASSERT_EQ(0, CurrentMetrics::get(CurrentMetrics::S3SessionInPool));

        wait_until([&] () { return getServer().currentConnections() == 1; });
        ASSERT_EQ(1, getServer().currentConnections());

        echoRequest("Hello", *session);

        ASSERT_EQ(1, getServer().totalConnections());
        ASSERT_EQ(1, getServer().currentConnections());

        std::cerr << "implicit reset session" << std::endl;
    }

    ASSERT_EQ(0, CurrentMetrics::get(CurrentMetrics::S3SessionActive));
    ASSERT_EQ(0, CurrentMetrics::get(CurrentMetrics::S3SessionInPool));

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionCreated]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionPreserved]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionReused]);
}

TEST_F(SessionPoolTest, CanReuse10)
{
    auto pool = makePool();


    for (int i = 0; i < 10; ++i)
    {
        auto session = pool->getSession();
        echoRequest("Hello", *session);
        setReuseTag(*session);
    }

    {
        [[maybe_unused]] auto tmp = pool->getSession();
    }

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionCreated]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionPreserved]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionReused]);
}

TEST_F(SessionPoolTest, CanReconnectAndCreate)
{
    auto pool = makePool();

    std::vector<HTTPSessionPtr> in_use;

    const size_t count = 2;
    for (int i = 0; i < count; ++i)
    {
        auto session = pool->getSession();
        setReuseTag(*session);
        in_use.push_back(session);
    }

    ASSERT_EQ(count, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionCreated]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionPreserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionReused]);

    ASSERT_EQ(count, CurrentMetrics::get(CurrentMetrics::S3SessionActive));
    ASSERT_EQ(0, CurrentMetrics::get(CurrentMetrics::S3SessionInPool));

    auto session = std::move(in_use.back());
    in_use.pop_back();

    echoRequest("Hello", *session);

    session->abort(); // further usage requires reconnect, new connection

    echoRequest("Hello", *session);

    session->reset();

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());
    ASSERT_EQ(count+1, getServer().totalConnections());

    ASSERT_EQ(count+1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionCreated]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionPreserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionReused]);
}

TEST_F(SessionPoolTest, CanReconnectAndReuse)
{
    auto pool = makePool();

    std::vector<HTTPSessionPtr> in_use;

    const size_t count = 2;
    for (int i = 0; i < count; ++i)
    {
        auto session = pool->getSession();
        setReuseTag(*session);
        in_use.push_back(std::move(session));
    }

    auto session = std::move(in_use.back());
    in_use.pop_back();
    in_use.clear(); // other session will be reused

    echoRequest("Hello", *session);

    session->abort(); // further usage requires reconnect, reuse connection from pool

    echoRequest("Hello", *session);

    session->reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(2, getServer().totalConnections());

    ASSERT_EQ(count, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionCreated]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionPreserved]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[ProfileEvents::S3SessionReused]);
}
