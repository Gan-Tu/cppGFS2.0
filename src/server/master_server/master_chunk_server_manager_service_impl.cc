#include "src/server/master_server/master_chunk_server_manager_service_impl.h"

#include <memory>

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/master_chunk_server_manager_service.grpc.pb.h"
#include "src/server/master_server/chunk_server_manager.h"

using grpc::ServerContext;
using protos::grpc::ReportChunkServerReply;
using protos::grpc::ReportChunkServerRequest;

namespace gfs {
namespace service {

// Creates a flat_hash_set from a protobuf RepeatedPtrField.
// The type T must be hashable.
// TODO(bmokutub): Consider moving to utils
template <class T>
absl::flat_hash_set<T> CreateFlatSetFromRepeatedPtrField(
    const google::protobuf::RepeatedPtrField<T>& repeated_ptr_field) {
  absl::flat_hash_set<T> output_set;

  for (int i = 0; i < repeated_ptr_field.size(); ++i) {
    output_set.insert(repeated_ptr_field[i]);
  }

  return output_set;
}

grpc::Status MasterChunkServerManagerServiceImpl::ReportChunkServer(
    ServerContext* context, const ReportChunkServerRequest* request,
    ReportChunkServerReply* reply) {
  auto& new_server_info = request->chunk_server();
  auto existing_server_info =
      gfs::server::ChunkServerManager::GetInstance().GetChunkServer(
          new_server_info.location());

  if (!existing_server_info.has_location()) {
    // Not found, new server info, maybe chunkserver is just starting up or
    // restarted. Or the master just started or crashed/restarted and lost all
    // in-mem chunkservermgr record. Lets register this chunkserver.
    std::shared_ptr<protos::ChunkServer> new_chunk_server(
        new protos::ChunkServer(new_server_info));

    gfs::server::ChunkServerManager::GetInstance().RegisterChunkServer(
        new_chunk_server);

  } else {
    // The chunkserver already exist, so we need to compare the existing info
    // with new info and update.

    // The reported chunks that we don't know about.
    // First set all the reported chunks as chunks to add to our chunkserver
    // info.
    absl::flat_hash_set<std::string> chunks_to_add =
        CreateFlatSetFromRepeatedPtrField<std::string>(
            request->chunk_server().stored_chunk_handles());

    // The chunks that we think exist on the chunkserver but no longer exist
    // on it.
    absl::flat_hash_set<std::string> chunks_to_remove;

    // Compare with our stored chunk handles for the chunk server. To see
    // which reported chunks we have or don't have.
    for (int i = 0; i < existing_server_info.stored_chunk_handles_size(); ++i) {
      auto current_chunk_handle = existing_server_info.stored_chunk_handles(i);

      if (chunks_to_add.contains(current_chunk_handle)) {
        // Chunk server also reported this chunk. We know about this chunk,
        // no need to add. We remove it from the add set. We will know what
        // reported chunks weren't found in chunkservermgr at the end of the
        // iteration.
        // TODO(bmokutub): Check with metadata mgr that reported version
        // matches the current version if it doesn't match, if report version is
        // old, add to stale chunks in reply so chunkserver can delete it. And
        // remove this chunkserver from the chunk locations in chunkservermgr,
        // so we don't return this location as chunk location. If reported
        // version is newer, that means master crashed after asking all
        // chunkservers to advance chunk version before a write, so ask metadata
        // mgr to advance the version to the new version.
        chunks_to_add.erase(current_chunk_handle);
      } else {
        // This means chunk server doesn't have this chunk stored on it.
        // Maybe crashed during write or disk got corrupted.
        // Add this chunk to the set of chunks to remove from chunkserver.
        // Chunkserver will be removed from chunklocation map and update
        // chunkserver info not to include this chunk
        //
        // NOTE: Lets watch this, and watchout for a situation where the we
        // allocated a chunkserver for a chunk and before client connects to
        // chunkserver, the chunkserver reports chunks to master and this
        // willn't include the just allocated chunk since client has not
        // talked to the chunkserver, so we end up removing the chunk here
        // from the chunkserver and subsequent client calls will result in us
        // not including this chunkserver, (and this can possible happen to
        // all the allocated chunkserver) and end up returning no
        // chunklocation for the chunk when chunkservermgr is asked. This will
        // be fixed once the chunkserver makes another report since it will
        // now include this chunk and we will add it. The duration, where we
        // don't return this chunkserver for the chunk depends on the
        // chunkserver report interval. If this report is frequent, then this
        // will be fixed fast. If very frequent, then this situation mayn't be
        // noticed.
        chunks_to_remove.insert(current_chunk_handle);
      }
    }

    // At this point, if there is any chunk handle left in the
    // chunks_to_add, it means we don't know the chunk server has them
    // so let's update our record. This could happen if, master
    // crashed/restarted after allocating the chunk to chunk servers and started
    // immediately such that during the first report the chunkserver sent, the
    // write had not completed on the chunkserver so it didn't include it.

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
