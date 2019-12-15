namespace java pack.iscsi.brick.remote.generated

typedef i32 int
typedef i64 long

exception PackBrickException {
  1: string message,
  2: string stackTraceStr
}

struct CreateRequest {
  1:string brickId,
  2:int length
}

struct CreateResponse {

}

struct ReadRequest {
  1:string brickId,
  2:long position,
  3:int length
}

struct ReadResponse {
  1:long generationId,
  2:binary data
}

struct WriteRequest {
  1:string brickId,
  2:long position,
  3:binary data
}

struct WriteResponse {
  1:long generationId
}

struct DestroyRequest {
  1:string brickId
}

struct DestroyResponse {

}

struct ExistsRequest {
  1:string brickId
}

struct ExistsResponse {
  1:bool exists
}

struct ListBricksRequest {
  1:string prefix
}

struct ListBricksResponse {
  1:list<string> brickIds
}

service PackBrickService
{

  CreateResponse create(1:CreateRequest request) throws (1:PackBrickException pe)

  ReadResponse read(1:ReadRequest request) throws (1:PackBrickException pe)

  WriteResponse write(1:WriteRequest request) throws (1:PackBrickException pe)

  ListBricksResponse listBricks(1:ListBricksRequest request) throws (1:PackBrickException pe)

  DestroyResponse destroy(1:DestroyRequest request) throws (1:PackBrickException pe)

  ExistsResponse exists(1:ExistsRequest request) throws (1:PackBrickException pe)

  void noop() throws (1:PackBrickException pe)

}
