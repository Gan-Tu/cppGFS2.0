#include "src/server/master_server/master_chunk_server_manager_service_impl.h"

#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "grpcpp/grpcpp.h"
#include "src/common/protos_common.h"
#include "src/common/system_logger.h"
#include "src/protos/grpc/master_chunk_server_manager_service.grpc.pb.h"
#include "src/server/master_server/chunk_server_manager.h"
#include "src/server/master_server/metadata_manager.h"

using grpc::ServerContext;
using protos::grpc::ReportChunkServerReply;
using protos::grpc::ReportChunkServerRequest;

namespace gfs {
namespace service {

grpc::Status MasterChunkServerManagerServiceImpl::ReportChunkServer(
    ServerContext* context, const ReportChunkServerRequest* request,
    ReportChunkServerReply* reply) {
  auto& new_server_info = request->chunk_server();
  LOG(INFO) << "Master handling report from " << new_server_info.DebugString();

  auto* metadata_manager = gfs::server::MetadataManager::GetInstance();

  auto existing_server_info =
      gfs::server::ChunkServerManager::GetInstance().GetChunkServer(
          new_server_info.location());

  // Whether a chunk the master doesn't know can be assumed deleted. With
  // persistence enabled, the master's (recovered) metadata is authoritative,
  // so an unknown chunk is garbage from a deleted file or a failed creation
  // (GFS paper section 4.4). WITHOUT persistence, a master restart forgets
  // every chunk, so on a chunk server's (re-)registration the report must be
  // trusted instead — otherwise a restarted master would order the whole
  // cluster's data deleted.
  const bool unknown_chunk_means_deleted =
      metadata_manager->HasPersistence() || existing_server_info.has_location();

  // The versions the chunk server reported for its stored chunks. Reports
  // from chunk servers always carry versions; if a report doesn't (e.g. an
  // old client), the version checks below are skipped for those chunks.
  absl::flat_hash_map<std::string, uint32_t> reported_versions;
  for (const auto& reported_chunk : request->stored_chunks()) {
    reported_versions[reported_chunk.chunk_handle()] = reported_chunk.version();
  }

  // Decide, for every chunk the server reported, whether the master still
  // considers this replica valid (GFS paper section 4.5):
  // - a chunk that no longer exists in the master's metadata was deleted;
  //   the chunk server is free to delete its replica (section 4.4)
  // - a replica whose version is *behind* the master's is stale (it missed
  //   mutations while its server was down); it must not be served to
  //   clients, and the chunk server is told to delete it
  // - a replica whose version is *ahead* of the master's means the master
  //   failed after instructing replicas to advance but before recording the
  //   new version; the master takes the higher version to be up to date
  absl::flat_hash_set<std::string> valid_reported_chunks;
  for (const auto& stored_chunk_handle :
       new_server_info.stored_chunk_handles()) {
    // Serialize against the write path's version-advance/lease-grant
    // sequence for this chunk, so the comparison below cannot interleave
    // with a concurrent advance and clobber (or misjudge) the version
    absl::MutexLock lease_lock_guard(
        metadata_manager->GetChunkLeaseLock(stored_chunk_handle));

    auto chunk_metadata_or =
        metadata_manager->GetFileChunkMetadata(stored_chunk_handle);
    if (!chunk_metadata_or.ok()) {
      if (unknown_chunk_means_deleted) {
        LOG(INFO) << "Chunk handle " << stored_chunk_handle
                  << " no longer exists in the master's metadata; telling "
                  << "chunk server to garbage collect it";
        *reply->add_stale_chunk_handles() = stored_chunk_handle;
      } else {
        // Persistence-less master restart: trust the report
        valid_reported_chunks.insert(stored_chunk_handle);
      }
      continue;
    }

    if (reported_versions.contains(stored_chunk_handle)) {
      const uint32_t reported_version =
          reported_versions.at(stored_chunk_handle);
      const uint32_t master_version = chunk_metadata_or.value().version();
      if (reported_version < master_version) {
        // Order deletion only if another replica still exists somewhere:
        // if every up-to-date replica has been lost, this stale copy is the
        // last surviving data for the chunk, and destroying it would turn a
        // stale-data situation into irrecoverable loss. It is still dropped
        // from the serving locations below (clients never read stale data,
        // GFS paper section 4.5) but stays on disk.
        bool another_replica_exists = false;
        for (const auto& location :
             gfs::server::ChunkServerManager::GetInstance().GetChunkLocations(
                 stored_chunk_handle)) {
          if (!(location == new_server_info.location())) {
            another_replica_exists = true;
            break;
          }
        }
        if (another_replica_exists) {
          LOG(INFO) << "Chunk handle " << stored_chunk_handle << " at version "
                    << reported_version << " is stale (master has version "
                    << master_version
                    << "); telling chunk server to delete it";
          *reply->add_stale_chunk_handles() = stored_chunk_handle;
        } else {
          LOG(ERROR) << "Chunk handle " << stored_chunk_handle
                     << " is stale (v" << reported_version << " < master's v"
                     << master_version << ") but is the last surviving "
                     << "replica; retaining it on disk without serving it";
        }
        continue;
      }
      if (reported_version > master_version) {
        LOG(WARNING) << "Chunk handle " << stored_chunk_handle
                     << " reported at version " << reported_version
                     << " which is newer than the master's record "
                     << master_version
                     << "; adopting the higher version as up to date";
        metadata_manager
            ->SetChunkVersion(stored_chunk_handle, reported_version)
            .IgnoreError();
      }
    }

    valid_reported_chunks.insert(stored_chunk_handle);
  }

  if (!existing_server_info.has_location()) {
    // Not found, new server info, maybe chunkserver is just starting up or
    // restarted. Or the master just started or crashed/restarted and lost all
    // in-mem chunkservermgr record. Lets register this chunkserver, with only
    // the chunks the master considers valid replicas.
    std::shared_ptr<protos::ChunkServer> new_chunk_server(
        new protos::ChunkServer(new_server_info));
    new_chunk_server->clear_stored_chunk_handles();
    for (const auto& chunk_handle : valid_reported_chunks) {
      new_chunk_server->add_stored_chunk_handles(chunk_handle);
    }

    gfs::server::ChunkServerManager::GetInstance().RegisterChunkServer(
        new_chunk_server);

  } else {
    // The chunkserver already exist, so we need to compare the existing info
    // with new info and update.

    // The valid reported chunks that we don't know about yet.
    absl::flat_hash_set<std::string> chunks_to_add = valid_reported_chunks;

    // The chunks that we think exist on the chunkserver but that it either
    // no longer reports, or that the master just declared stale.
    absl::flat_hash_set<std::string> chunks_to_remove;

    // Compare with our stored chunk handles for the chunk server. To see
    // which reported chunks we have or don't have.
    for (int i = 0; i < existing_server_info.stored_chunk_handles_size(); ++i) {
      auto current_chunk_handle = existing_server_info.stored_chunk_handles(i);

      if (chunks_to_add.contains(current_chunk_handle)) {
        // Chunk server also reported this chunk, and the master considers
        // the replica valid. We already know about it, no need to add.
        chunks_to_add.erase(current_chunk_handle);
      } else {
        // The chunk server no longer has this chunk (crashed during write,
        // disk corrupted, ...), or the master just declared this replica
        // stale/deleted above. Remove it from the chunk server's record and
        // from the chunk location map so clients are no longer directed to
        // this replica.
        chunks_to_remove.insert(current_chunk_handle);
      }
    }

    // At this point, if there is any chunk handle left in the
    // chunks_to_add, it means we don't know the chunk server has them
    // so let's update our record. This could happen if master
    // crashed/restarted after allocating the chunk to chunk servers, or if
    // this server just finished re-replicating a chunk.

    gfs::server::ChunkServerManager::GetInstance().UpdateChunkServer(
        new_server_info.location(), new_server_info.available_disk_mb(),
        chunks_to_add, chunks_to_remove);
  }

  // Prepare reply
  *reply->mutable_request() = *request;

  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gfs
