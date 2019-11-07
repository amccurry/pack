namespace java pack.iscsi.brick.remote.generated

typedef i32 int
typedef i64 long

exception PackBrickException {
  1: string message,
  2: string stackTraceStr
}

struct CreateRequest {
  1:long brickId,
  2:long length
}

struct CreateResponse {

}

struct ReadRequest {
  1:long brickId,
  2:long position,
  3:int length
}

struct ReadResponse {
  1:binary data
}

struct WriteRequest {
  1:long brickId,
  2:long position,
  3:binary data
}

struct WriteResponse {

}

struct DestroyRequest {
  1:long brickId
}

struct DestroyResponse {

}

struct ListBricksRequest {

}

struct ListBricksResponse {
  1:list<long> brickIds
}

struct TraceRequest {
  1:string traceId
}

service PackBrickService
{
  CreateResponse create(1:CreateRequest request) throws (1:PackBrickException pe)

  ReadResponse read(1:ReadRequest request) throws (1:PackBrickException pe)

  WriteResponse write(1:WriteRequest request) throws (1:PackBrickException pe)

  oneway void writeNonBlocking(1:WriteRequest request)

  ListBricksResponse listBricks(1:ListBricksRequest request) throws (1:PackBrickException pe)

  DestroyResponse destroy(1:DestroyRequest request) throws (1:PackBrickException pe)

  void noop() throws (1:PackBrickException pe)

}
