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

struct JournalRangeResponse {
  1:list<JournalRange> journalRangeList
}

struct JournalRangeRequest {
  1:long volumeId,
  2:long blockId,
  3:long onDiskGeneration,
  4:bool closeExistingWriter
}

struct JournalRange {
  1:long volumeId,
  2:long blockId,
  3:string uuid,
  4:long minGeneration,
  5:long maxGeneration
}

struct FetchJournalEntriesRequest {
  1:long volumeId,
  2:long blockId,
  3:string uuid,
  4:long maxGeneration,
  5:long minGeneration,
  6:long onDiskGeneration
}

struct FetchJournalEntriesResponse {
  1:long volumeId,
  2:long blockId,
  3:bool journalExhausted,
  4:list<JournalEntry> entries
}

struct JournalEntry {
  1:long generation,
  2:long position,
  3:binary data
}

exception PackException {
  1: string message,
  2: string stackTraceStr
}

service PackWalService
{
  void ping() throws (1:PackException pe)

  void write(1:WriteRequest writeRequest) throws (1:PackException pe)

  void releaseJournals(1:ReleaseRequest releaseRequest) throws (1:PackException pe)

  JournalRangeResponse journalRanges(1:JournalRangeRequest journalRangeRequest) throws (1:PackException pe)

  FetchJournalEntriesResponse fetchJournalEntries(1:FetchJournalEntriesRequest fetchJournalEntriesRequest) throws (1:PackException pe)

}
