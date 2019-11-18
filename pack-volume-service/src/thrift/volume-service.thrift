namespace java pack.volume.remote.generated

typedef i32 int
typedef i64 long

exception PackVolumeException {
  1: string message,
  2: string stackTraceStr
}

struct VolumeInfo {
  1:string volumeName,
  2:long volumeId,
  3:int blockSize
  4:long blockCount
}

struct CreateRequest {
  1:VolumeInfo volumeInfo
}

struct CreateResponse {

}

struct UpdateRequest {
  1:VolumeInfo volumeInfo
}

struct UpdateResponse {

}

struct ReadRequest {
  1:long volumeId,
  2:long position,
  3:int length
}

struct ReadResponse {
  1:binary data
}

struct WriteRequest {
  1:long volumeId,
  2:long position,
  3:binary data
}

struct WriteResponse {

}

struct DiscardRequest {
  1:long volumeId,
  2:long position,
  3:long length
}

struct DiscardResponse {

}

struct DestroyRequest {
  1:long volumeId
}

struct DestroyResponse {

}

struct ExistsRequest {
  1:long volumeId,
  2:string volumeName
}

struct ExistsResponse {
  1:VolumeInfo volumeInfo
}

struct ListRequest {
  1:string volumeNamePrefix
}

struct ListResponse {
  1:list<VolumeInfo> volumeInfos
}

service PackBrickService
{

  CreateResponse createVolume(1:CreateRequest request) throws (1:PackVolumeException pe)

  UpdateResponse updateVolume(1:UpdateRequest request) throws (1:PackVolumeException pe)

  ListResponse listVolume(1:ListRequest request) throws (1:PackVolumeException pe)

  DestroyResponse destroyVolume(1:DestroyRequest request) throws (1:PackVolumeException pe)

  ExistsResponse existsVolume(1:ExistsRequest request) throws (1:PackVolumeException pe)

  ReadResponse readVolume(1:ReadRequest request) throws (1:PackVolumeException pe)

  WriteResponse writeVolume(1:WriteRequest request) throws (1:PackVolumeException pe)

  DiscardResponse discardVolume(1:DiscardRequest request) throws (1:PackVolumeException pe)

  void noop() throws (1:PackVolumeException pe)

}
