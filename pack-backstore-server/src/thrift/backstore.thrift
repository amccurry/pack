namespace java pack.backstore.file.thrift.generated

typedef i32 int
typedef i64 long

enum BackstoreError {
  UNKNOWN = 0,
  FILE_DELETE_FAILED = 1,
  FILE_NOT_FOUND = 2,
  FILE_EXISTS = 3,
  IO_ERROR = 4
}   

exception BackstoreFileServiceException {
  1:BackstoreError errorType,
  2:string message,
  3:string stackTraceStr
}

struct CreateFileRequest {
  1:string filename,
  2:int length
}

struct ReadFileRequestBatch {
  1:string filename,
  2:list<ReadFileRequest> readRequests
}

struct ReadFileRequest {
  1:long position,
  2:int length
}

struct ReadFileResponseBatch {
  1:list<ReadFileResponse> readResponses
}

struct ReadFileResponse {
  1:binary data
}

struct WriteFileRequestBatch {
  1:string filename,
  2:list<WriteFileRequest> writeRequests
}

struct WriteFileRequest {
  1:long position,
  2:binary data
}

struct DestroyFileRequest {
  1:string filename
}

struct ExistsFileRequest {
  1:string filename
}

struct ExistsFileResponse {
  1:bool exists
}

struct ListFilesRequest {
  1:string prefix
}

struct ListFilesResponse {
  1:list<string> filenames
}

service BackstoreFileService
{
  void create(1:CreateFileRequest request) throws (1:BackstoreFileServiceException pe)

  ReadFileResponseBatch read(1:ReadFileRequestBatch request) throws (1:BackstoreFileServiceException pe)

  void write(1:WriteFileRequestBatch request) throws (1:BackstoreFileServiceException pe)

  ListFilesResponse listFiles(1:ListFilesRequest request) throws (1:BackstoreFileServiceException pe)

  void destroy(1:DestroyFileRequest request) throws (1:BackstoreFileServiceException pe)

  ExistsFileResponse exists(1:ExistsFileRequest request) throws (1:BackstoreFileServiceException pe)

  void noop() throws (1:BackstoreFileServiceException pe)

}
