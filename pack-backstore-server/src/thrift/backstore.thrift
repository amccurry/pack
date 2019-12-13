namespace java pack.backstore.thrift.generated

typedef i32 int
typedef i64 long
typedef bool boolean

enum BackstoreError {
  UNKNOWN = 0,
  FILE_DELETE_FAILED = 1,
  FILE_NOT_FOUND = 2,
  FILE_EXISTS = 3,
  IO_ERROR = 4,
  FILENAME_MISSING = 5,
  LOCK_MISSING = 6,
  LOCK_ALREADY_REGISTERED = 7,
  LOCK_ALREADY_INVALID = 8,
  LOCK_ID_MISSING = 9
}

exception BackstoreServiceException {
  1:BackstoreError errorType,
  2:string message,
  3:string stackTraceStr
}

/*
---------------------------------------------------------------
File Service Section
---------------------------------------------------------------
*/


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
  2:string lockId,
  3:list<WriteFileRequest> writeRequests
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
  void create(1:CreateFileRequest request) throws (1:BackstoreServiceException e)

  ReadFileResponseBatch read(1:ReadFileRequestBatch request) throws (1:BackstoreServiceException e)

  void write(1:WriteFileRequestBatch request) throws (1:BackstoreServiceException e)

  ListFilesResponse listFiles(1:ListFilesRequest request) throws (1:BackstoreServiceException e)

  void destroy(1:DestroyFileRequest request) throws (1:BackstoreServiceException e)

  ExistsFileResponse exists(1:ExistsFileRequest request) throws (1:BackstoreServiceException e)

  void noop() throws (1:BackstoreServiceException e)

}

/*
---------------------------------------------------------------
Coordinator Service Section
---------------------------------------------------------------
*/

struct FileLockInfoRequest {
  1:string filename
}

struct FileLockInfoResponse {
  1:string lockId
}

struct RegisterFileRequest {
  1:string filename
}

struct RegisterFileResponse {
  1:string lockId
}

struct ReleaseFileRequest {
  1:string filename,
  2:string lockId
}

service BackstoreCoordinatorService
{

  FileLockInfoResponse fileLock(1:FileLockInfoRequest request) throws (1:BackstoreServiceException e)

  RegisterFileResponse registerFileLock(1:RegisterFileRequest request) throws (1:BackstoreServiceException e)

  void releaseFileLock(1:ReleaseFileRequest request) throws (1:BackstoreServiceException e)

  void noop() throws (1:BackstoreServiceException e)

}
