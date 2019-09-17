namespace java pack.iscsi.wal.remote.generated

typedef i32 int
typedef i64 long

struct WriteRequest {
  1:long volumeId,
  2:long blockId,
  3:long generation,
  4:long position,
  5:binary data
}

struct ReleaseRequest {
  1:long volumeId,
  2:long blockId,
  3:long generation
}

struct MaxGenerationRequest {
  1:long volumeId,
  2:long blockId
}

struct MaxGenerationResponse {
  1:long generation
}

struct RecoverRequest {
  1:long volumeId,
  2:long blockId,
  3:long generationStart,
  4:long generationEnd
}

struct RecoverData {
  1:long volumeId,
  2:long blockId,
  3:long generation,
  4:long position,
  5:binary data
}

struct RecoverResponse {
  1:list<RecoverData> recoverDataList
}

exception PackException {
  1: string message
}

service PackWalService
{
  void write(1:WriteRequest writeRequest) throws (1:PackException pe)

  void release(1:ReleaseRequest releaseRequest) throws (1:PackException pe)

  MaxGenerationResponse maxGeneration(1:MaxGenerationRequest maxGenerationRequest) throws (1:PackException pe)

  RecoverResponse recover(1:RecoverRequest recoverRequest) throws (1:PackException pe)

}
