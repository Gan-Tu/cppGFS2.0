#include "src/server/master_server/chunk_server_manager.h"

#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

using namespace gfs::server;

// The fixture for testing ChunkServerManager. Handles setup and cleanup for the
// test methods.
class ChunkServerManagerTest : public ::testing::Test {
 protected:
  // Executed after each test method in the suite.
  void TearDown() override {
    // Unregister all the ChunkServers registered by the test.
    ChunkServerManager::GetInstance().UnRegisterAllChunkServers();
  }
};

// Test helper to create chunkserverlocation
protos::ChunkServerLocation CreateChunkServerLocation(
    const std::string& host_name, const uint32_t& port) {
  protos::ChunkServerLocation server_location;

  server_location.set_server_hostname(host_name);
  server_location.set_server_port(port);

  return server_location;
}

// Test registering a chunk server with no chunks. Happens when a new
// chunkserver is being registered.
TEST_F(ChunkServerManagerTest, RegisterChunkServerWithNoChunks) {
  auto server_location = new protos::ChunkServerLocation(
      std::move(CreateChunkServerLocation("192.168.1.1", 3030)));

  std::shared_ptr<protos::ChunkServer> chunk_server(new protos::ChunkServer());
  chunk_server->set_allocated_location(server_location);
  EXPECT_TRUE(
      ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server));

  EXPECT_EQ(*chunk_server,
            ChunkServerManager::GetInstance().GetChunkServer(*server_location));
}

// Test registering a chunkserver with some stored chunks (happens when an
// existing chunkserver restarts) and then also unregister the chunkserver
// after.
TEST_F(ChunkServerManagerTest, RegisterAndUnregisterChunkServerWithChunks) {
  protos::ChunkServerLocation* server_location =
      new protos::ChunkServerLocation(
          std::move(CreateChunkServerLocation("192.168.1.1", 3030)));

  std::shared_ptr<protos::ChunkServer> chunk_server(new protos::ChunkServer());
  chunk_server->set_allocated_location(server_location);

  // Add some chunk handles to the chunk server
  const ushort chunk_count = 100;
  for (ushort i = 0; i < chunk_count; ++i) {
    chunk_server->add_stored_chunk_handles(std::to_string(i));
  }

  EXPECT_TRUE(
      ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server));

  EXPECT_EQ(*chunk_server,
            ChunkServerManager::GetInstance().GetChunkServer(*server_location));

  // check that the location of the chunks is our chunkserver
  for (ushort i = 0; i < chunk_count; ++i) {
    auto chunk_locations =
        ChunkServerManager::GetInstance().GetChunkLocations(std::to_string(i));

    EXPECT_EQ(1, chunk_locations.size());

    EXPECT_TRUE(chunk_locations.contains(*server_location));
  }

  // Now unregister the chunkserver
  ChunkServerManager::GetInstance().UnRegisterChunkServer(*server_location);

  // Trying to get the unregistered chunkserver should fail
  EXPECT_FALSE(ChunkServerManager::GetInstance()
                   .GetChunkServer(*server_location)
                   .has_location());

  // Verify that this chunkserver location no longer appears for the chunk
  // handles
  for (ushort i = 0; i < chunk_count; ++i) {
    auto chunk_locations =
        ChunkServerManager::GetInstance().GetChunkLocations(std::to_string(i));

    EXPECT_TRUE(chunk_locations.empty());
  }
}

// Test allocation of chunk to chunkserver and makes sure we prioritize
// chunkserver with max available disk.
TEST_F(ChunkServerManagerTest, AllocateChunkServer) {
  protos::ChunkServerLocation* small_server_location =
      new protos::ChunkServerLocation(
          std::move(CreateChunkServerLocation("192.168.1.1", 3030)));

  std::shared_ptr<protos::ChunkServer> small_chunk_server(
      new protos::ChunkServer());
  small_chunk_server->set_allocated_location(small_server_location);
  small_chunk_server->set_available_disk_mb(100);

  EXPECT_TRUE(ChunkServerManager::GetInstance().RegisterChunkServer(
      small_chunk_server));

  protos::ChunkServerLocation* big_server_location =
      new protos::ChunkServerLocation(
          std::move(CreateChunkServerLocation("192.168.100.1", 6060)));

  std::shared_ptr<protos::ChunkServer> big_chunk_server(
      new protos::ChunkServer());
  big_chunk_server->set_allocated_location(big_server_location);
  big_chunk_server->set_available_disk_mb(200);

  EXPECT_TRUE(
      ChunkServerManager::GetInstance().RegisterChunkServer(big_chunk_server));

  auto allocated_locations =
      ChunkServerManager::GetInstance().AllocateChunkServer(
          /*chunk_handle=*/"TestHandle", /*server_count=*/1);

  EXPECT_EQ(1, allocated_locations.size());

  EXPECT_TRUE(allocated_locations.contains(*big_server_location));
}

// Test allocation of multiple chunkservers for a chunk. Chunk replication
// scenario.
TEST_F(ChunkServerManagerTest, AllocateMultipleChunkServers) {
  const ushort chunk_servers_count = 10;
  for (ushort i = 0; i < chunk_servers_count; ++i) {
    protos::ChunkServerLocation* server_location =
        new protos::ChunkServerLocation(std::move(CreateChunkServerLocation(
            std::string("192.168.1.") + std::to_string(i), i)));

    std::shared_ptr<protos::ChunkServer> chunk_server(
        new protos::ChunkServer());
    chunk_server->set_allocated_location(server_location);
    // Giving 100 to 109mb here so each chunkserver would only be able to store
    // only 1 chunk (since chunksize is 64mb)
    chunk_server->set_available_disk_mb(100 + i);

    EXPECT_TRUE(
        ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server));
  }

  // Each chunk handle will be asking for 5 chunkservers for allocation.
  // Since chunkservers are allocated by available disk, the first chunk handles
  // should get chunkservers 9 to 5, and the second chunk handle should get
  // chunkservers 4 to 0.
  const ushort chunk_handles_count = 2;
  const ushort servers_to_request_count =
      chunk_servers_count / chunk_handles_count;
  ushort current_chunk_server_id = 9;
  for (ushort i = 0; i < chunk_handles_count; ++i) {
    auto allocated_locations =
        ChunkServerManager::GetInstance().AllocateChunkServer(
            std::string("TestHandle") + std::to_string(i),
            servers_to_request_count);

    EXPECT_EQ(servers_to_request_count, allocated_locations.size());

    for (ushort j = 0; j < servers_to_request_count; ++j) {
      protos::ChunkServerLocation server_location = CreateChunkServerLocation(
          std::string("192.168.1.") + std::to_string(current_chunk_server_id),
          current_chunk_server_id);

      EXPECT_TRUE(allocated_locations.contains(server_location));

      --current_chunk_server_id;
    }
  }

  // At this point all the chunkservers should be full and any allocation
  // request shouldn't succeed
  auto allocated_locations =
      ChunkServerManager::GetInstance().AllocateChunkServer("TestHandle_",
                                                            /*server_count=*/1);

  EXPECT_TRUE(allocated_locations.empty());
}

// Test updating a chunkserver.
TEST_F(ChunkServerManagerTest, UpdateChunkServer) {
  protos::ChunkServerLocation* server_location =
      new protos::ChunkServerLocation(
          std::move(CreateChunkServerLocation("192.168.1.1", 3030)));

  std::shared_ptr<protos::ChunkServer> chunk_server(new protos::ChunkServer());
  chunk_server->set_allocated_location(server_location);

  // Add some chunk handles to the chunk server
  const ushort chunk_count = 10;
  for (ushort i = 0; i < chunk_count; ++i) {
    chunk_server->add_stored_chunk_handles(std::to_string(i));
  }

  EXPECT_TRUE(
      ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server));

  EXPECT_EQ(*chunk_server,
            ChunkServerManager::GetInstance().GetChunkServer(*server_location));

  const uint32_t available_disk_mb = 100;

  absl::flat_hash_set<std::string> chunks_to_add;
  chunks_to_add.insert("10");
  chunks_to_add.insert("11");

  absl::flat_hash_set<std::string> chunks_to_remove;
  chunks_to_remove.insert("4");
  chunks_to_remove.insert("0");
  chunks_to_remove.insert("3");
  chunks_to_remove.insert("1");
  chunks_to_remove.insert("2");

  ChunkServerManager::GetInstance().UpdateChunkServer(
      *server_location, available_disk_mb, chunks_to_add, chunks_to_remove);

  // check that the chunks are updated
  // we have removed 0 to 4 and added 10 and 11,
  // so there should be chunks 5 to 11
  const ushort total_chunks_count = 12;
  for (ushort i = 0; i < total_chunks_count; ++i) {
    auto chunk_handle = std::to_string(i);
    auto chunk_locations =
        ChunkServerManager::GetInstance().GetChunkLocations(chunk_handle);

    if (chunks_to_remove.contains(chunk_handle)) {
      EXPECT_TRUE(chunk_locations.empty());
    } else {
      EXPECT_EQ(1, chunk_locations.size());

      EXPECT_TRUE(chunk_locations.contains(*server_location));
    }
  }

  // Check that the available disk was updated
  auto allocated_locations =
      ChunkServerManager::GetInstance().AllocateChunkServer("TestHandle_",
                                                            /*server_count=*/1);

  EXPECT_EQ(1, allocated_locations.size());

  EXPECT_TRUE(allocated_locations.contains(*server_location));
}

// Concurrency test

// Test concurrent chunkserver registration, then concurrent chunkserver
// unregister.
TEST_F(ChunkServerManagerTest,
       ConcurrentRegisterAndConcurrentUnregisterChunkServers) {
  ushort thread_count = 10;
  std::vector<std::thread> threads;

  const ushort chunk_count = 100;

  for (ushort i = 0; i < thread_count; i++) {
    threads.push_back(std::thread([&, i, chunk_count]() {
      protos::ChunkServerLocation* server_location =
          new protos::ChunkServerLocation(std::move(CreateChunkServerLocation(
              std::string("192.168.1.") + std::to_string(i), i)));

      std::shared_ptr<protos::ChunkServer> chunk_server(
          new protos::ChunkServer());
      chunk_server->set_allocated_location(server_location);
      chunk_server->set_available_disk_mb(i);

      // Add same chunk handles to the chunk server
      for (ushort i = 0; i < chunk_count; ++i) {
        chunk_server->add_stored_chunk_handles(std::to_string(i));
      }

      ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server);
    }));
  }

  // Join all threads
  for (ushort i = 0; i < thread_count; i++) {
    threads[i].join();
  }

  // Check that the chunkservers were registered
  for (ushort i = 0; i < thread_count; i++) {
    protos::ChunkServerLocation server_location = CreateChunkServerLocation(
        std::string("192.168.1.") + std::to_string(i), i);

    EXPECT_TRUE(ChunkServerManager::GetInstance()
                    .GetChunkServer(server_location)
                    .has_location());
  }

  // Check that the chunkservers were correctly mapped to the chunks
  for (ushort i = 0; i < chunk_count; i++) {
    auto chunk_locations =
        ChunkServerManager::GetInstance().GetChunkLocations(std::to_string(i));

    EXPECT_EQ(thread_count, chunk_locations.size());

    // Check that all the chunkservers are there
    for (ushort i = 0; i < thread_count; i++) {
      protos::ChunkServerLocation server_location = CreateChunkServerLocation(
          std::string("192.168.1.") + std::to_string(i), i);

      EXPECT_TRUE(chunk_locations.contains(server_location));
    }
  }

  // Remove the previous threads
  threads.clear();

  // now lets concurrently unregister all
  for (ushort i = 0; i < thread_count; i++) {
    threads.push_back(std::thread([&, i]() {
      protos::ChunkServerLocation server_location = CreateChunkServerLocation(
          std::string("192.168.1.") + std::to_string(i), i);

      ChunkServerManager::GetInstance().UnRegisterChunkServer(server_location);
    }));
  }

  // Join all threads
  for (ushort i = 0; i < thread_count; i++) {
    threads[i].join();
  }

  // Check that the chunkservers were unregistered
  for (ushort i = 0; i < thread_count; i++) {
    protos::ChunkServerLocation server_location = CreateChunkServerLocation(
        std::string("192.168.1.") + std::to_string(i), i);

    EXPECT_FALSE(ChunkServerManager::GetInstance()
                     .GetChunkServer(server_location)
                     .has_location());
  }

  // Check that the chunkservers were correctly Unmapped to the chunks
  for (ushort i = 0; i < chunk_count; i++) {
    auto chunk_locations =
        ChunkServerManager::GetInstance().GetChunkLocations(std::to_string(i));

    EXPECT_TRUE(chunk_locations.empty());
  }
}

// Test concurrent allocation of chunks to chunkservers
TEST_F(ChunkServerManagerTest, ConcurrentAllocateChunkServers) {
  const ushort chunk_servers_count = 10;
  ChunkServerLocationFlatSet created_locations;
  // Register chunkservers
  for (ushort i = 0; i < chunk_servers_count; i++) {
    protos::ChunkServerLocation* server_location =
        new protos::ChunkServerLocation(std::move(CreateChunkServerLocation(
            std::string("192.168.1.") + std::to_string(i), i)));

    // store all the created locations
    created_locations.insert(*server_location);

    std::shared_ptr<protos::ChunkServer> chunk_server(
        new protos::ChunkServer());
    chunk_server->set_allocated_location(server_location);
    // Giving them 100 to 109mb, can only store 1 chunk
    chunk_server->set_available_disk_mb(100 + i);

    EXPECT_TRUE(
        ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server));

    EXPECT_EQ(*chunk_server, ChunkServerManager::GetInstance().GetChunkServer(
                                 *server_location));
  }

  // concurrently allocate chunkservers
  ushort thread_count = 10;
  std::vector<std::future<ChunkServerLocationThreadSafeFlatSet>> thread_futures;

  for (ushort i = 0; i < thread_count; i++) {
    // Launch async to prevent deferred execution
    thread_futures.push_back(std::async(std::launch::async, [&, i]() {
      return ChunkServerManager::GetInstance().AllocateChunkServer(
          std::to_string(i),
          /*server_count=*/1);
    }));
  }

  // Wait for each thread future and get result
  for (ushort i = 0; i < thread_count; i++) {
    auto allocated_locations = thread_futures[i].get();

    // We requested for 1 server
    EXPECT_EQ(1, allocated_locations.size());

    // Get the location
    auto location = *(allocated_locations.begin());

    EXPECT_TRUE(created_locations.contains(location));

    // Remove to check that this location was only assigned to this thread's
    // chunk, since we configured each chunkserver with disk for just 1 chunk.
    // If we see this location then the check above will fail.
    created_locations.erase(location);

    // Ensure that GetChunkLocation returns this location
    auto chunk_locations =
        ChunkServerManager::GetInstance().GetChunkLocations(std::to_string(i));

    EXPECT_EQ(1, chunk_locations.size());

    EXPECT_EQ(location, *(chunk_locations.begin()));
  }
}

// Test concurrent chunkserver registration while also concurrently allocating
// chunkservers.
TEST_F(ChunkServerManagerTest, ConcurrentRegisterAndAllocateChunkServers) {
  const ushort chunk_servers_count = 10;
  const ushort preregistered_servers_count = chunk_servers_count / 2;
  const ushort concurrently_registered_servers_count = chunk_servers_count / 2;
  ChunkServerLocationThreadSafeFlatSet created_locations;

  // Pre-register 5 chunkservers
  for (ushort i = 0; i < preregistered_servers_count; i++) {
    protos::ChunkServerLocation* server_location =
        new protos::ChunkServerLocation(std::move(CreateChunkServerLocation(
            std::string("192.168.1.") + std::to_string(i), i)));

    // store all the created locations
    created_locations.insert(*server_location);

    std::shared_ptr<protos::ChunkServer> chunk_server(
        new protos::ChunkServer());
    chunk_server->set_allocated_location(server_location);
    // Giving them 100 to 109mb, can only store 1 chunk
    chunk_server->set_available_disk_mb(100 + i);

    EXPECT_TRUE(
        ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server));

    EXPECT_EQ(*chunk_server, ChunkServerManager::GetInstance().GetChunkServer(
                                 *server_location));
  }

  // concurrently register 5 more chunservers and allocate 8 chunkservers
  // Using 8 here since allocation is usually faster than registration.
  ushort server_allocation_thread_count = chunk_servers_count - 2;
  std::vector<std::future<ChunkServerLocationThreadSafeFlatSet>>
      server_allocation_thread_futures;

  std::vector<std::future<void>> server_registration_thread_futures;

  for (ushort i = 0; i < server_allocation_thread_count; i++) {
    // Launch async to prevent deferred execution
    server_allocation_thread_futures.push_back(
        std::async(std::launch::async, [&, i]() {
          return ChunkServerManager::GetInstance().AllocateChunkServer(
              std::to_string(i),
              /*server_count=*/1);
        }));

    // we only want to create 5 more chunkservers
    if (i < concurrently_registered_servers_count) {
      const ushort server_id = preregistered_servers_count + i;

      server_registration_thread_futures.push_back(
          std::async(std::launch::async, [&, server_id]() {
            protos::ChunkServerLocation* server_location =
                new protos::ChunkServerLocation(
                    std::move(CreateChunkServerLocation(
                        std::string("192.168.1.") + std::to_string(server_id),
                        server_id)));

            std::shared_ptr<protos::ChunkServer> chunk_server(
                new protos::ChunkServer());
            chunk_server->set_allocated_location(server_location);
            // Giving them 100 to 109mb, can only store 1 chunk
            chunk_server->set_available_disk_mb(100 + server_id);

            ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server);

            // store all the created locations.
            // Doing this here, since the set synchronizes threads and don't
            // want that before we register.
            created_locations.insert(*server_location);
          }));
    }
  }

  // Wait for each server allocation thread future and get result.
  // No need to wait for the chunkserver registration threads.
  for (ushort i = 0; i < server_allocation_thread_count; i++) {
    auto allocated_locations = server_allocation_thread_futures[i].get();

    // We requested for 1 server and every chunk should get 1 since we added 5
    // more to the preregistered 5.
    EXPECT_EQ(1, allocated_locations.size());

    // Get the location
    auto location = *(allocated_locations.begin());

    EXPECT_TRUE(created_locations.contains(location));

    // Remove to check that this location was only assigned to this thread's
    // chunk, since we configured each chunkserver with disk for just 1 chunk.
    // If we see this location then the check above will fail.
    created_locations.erase(location);

    // Ensure that GetChunkLocation returns this location
    auto chunk_locations =
        ChunkServerManager::GetInstance().GetChunkLocations(std::to_string(i));

    EXPECT_EQ(1, chunk_locations.size());

    EXPECT_EQ(location, *(chunk_locations.begin()));
  }
}

// Test concurrent allocation of chunk servers while also concurrently updating
// chunkservers.
TEST_F(ChunkServerManagerTest, ConcurrentAllocateAndUpdateChunkServers) {
  const ushort chunk_servers_count = 10;
  ChunkServerLocationFlatSet created_locations;

  // Register 10 chunkservers with chunks 101 to 110
  for (ushort i = 1; i <= chunk_servers_count; i++) {
    protos::ChunkServerLocation* server_location =
        new protos::ChunkServerLocation(std::move(CreateChunkServerLocation(
            std::string("192.168.1.") + std::to_string(i), i)));

    // store all the created locations
    created_locations.insert(*server_location);

    std::shared_ptr<protos::ChunkServer> chunk_server(
        new protos::ChunkServer());
    chunk_server->set_allocated_location(server_location);
    // Giving them 101 to 110mb, can only store 1 chunk
    chunk_server->set_available_disk_mb(100 + i);

    chunk_server->add_stored_chunk_handles(std::to_string(100 + i));

    EXPECT_TRUE(
        ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server));

    EXPECT_EQ(*chunk_server, ChunkServerManager::GetInstance().GetChunkServer(
                                 *server_location));
  }

  // Only doing 5 allocations, because we will be updating 5 servers disk to 0.
  ushort server_allocation_thread_count = 5;
  std::vector<std::future<ChunkServerLocationThreadSafeFlatSet>>
      server_allocation_thread_futures;

  // we only want to update the top 5 chunkservers (server 6 to 10)
  ushort server_update_thread_count = 5;
  std::vector<std::future<void>> server_update_thread_futures;

  for (ushort i = 0; i < server_allocation_thread_count; i++) {
    // Allocating chunks 0 to 4 here
    // Launch async to prevent deferred execution
    server_allocation_thread_futures.push_back(
        std::async(std::launch::async, [&, i]() {
          return ChunkServerManager::GetInstance().AllocateChunkServer(
              std::to_string(i),
              /*server_count=*/1);
        }));

    // we only want to update the top 5 chunkservers (server 6 to 10)
    if (i < server_update_thread_count) {
      const ushort server_id = chunk_servers_count - i;
      // Adding chunks 6 to 10 here
      const std::string& chunk_to_add = std::to_string(server_id);
      server_update_thread_futures.push_back(
          std::async(std::launch::async, [&, server_id, chunk_to_add]() {
            protos::ChunkServerLocation server_location =
                CreateChunkServerLocation(
                    std::string("192.168.1.") + std::to_string(server_id),
                    server_id);

            const uint32_t available_disk_mb = 0;

            absl::flat_hash_set<std::string> chunks_to_add;
            chunks_to_add.insert(chunk_to_add);

            // Remove the added chunk on registration
            absl::flat_hash_set<std::string> chunks_to_remove;
            chunks_to_remove.insert(std::to_string(100 + server_id));

            ChunkServerManager::GetInstance().UpdateChunkServer(
                server_location, available_disk_mb, chunks_to_add,
                chunks_to_remove);
          }));
    }
  }

  // Wait for each server allocation thread future and get result.
  // No need to wait for the chunkserver update threads.
  for (ushort i = 0; i < server_allocation_thread_count; i++) {
    auto allocated_locations = server_allocation_thread_futures[i].get();

    // We requested for 1 server and every chunk should get 1 since we added 5
    // more to the preregistered 5.
    EXPECT_EQ(1, allocated_locations.size());

    // Get the location
    auto location = *(allocated_locations.begin());

    EXPECT_TRUE(created_locations.contains(location));

    // Remove to check that this location was only assigned to this thread's
    // chunk, since we configured each chunkserver with disk for just 1 chunk.
    // If we see this location then the check above will fail.
    created_locations.erase(location);

    // Ensure that GetChunkLocation returns this location
    auto chunk_locations =
        ChunkServerManager::GetInstance().GetChunkLocations(std::to_string(i));

    EXPECT_EQ(1, chunk_locations.size());

    EXPECT_EQ(location, *(chunk_locations.begin()));
  }

  // Wait for all update to complete
  for (ushort i = 0; i < server_update_thread_count; i++) {
    server_update_thread_futures[i].wait();
  }

  // We add chunks 6 to 10 and removed chunks 106 to 110
  // from servers 6 to 10. Verify the update was correctly done.
  for (ushort server_id = 6; server_id <= chunk_servers_count; ++server_id) {
    auto added_chunk_handle = std::to_string(server_id);
    auto removed_chunk_handle = std::to_string(100 + server_id);

    protos::ChunkServerLocation server_location = CreateChunkServerLocation(
        std::string("192.168.1.") + std::to_string(server_id), server_id);

    auto chunk_locations =
        ChunkServerManager::GetInstance().GetChunkLocations(added_chunk_handle);

    EXPECT_EQ(1, chunk_locations.size());

    EXPECT_TRUE(chunk_locations.contains(server_location));

    EXPECT_TRUE(ChunkServerManager::GetInstance()
                    .GetChunkLocations(removed_chunk_handle)
                    .empty());
  }
}
