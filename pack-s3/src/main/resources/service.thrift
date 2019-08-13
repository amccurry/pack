namespace java pack.s3.thrift

service PackService
{

i32 openVolume(1:string volumeName),
binary readData(1:i32 fh, 2:i64 position),
void writeData(1:i32 fh, 2:i64 position, 3:binary buffer),
void deleteData(1:i32 fh, 2:i64 position, 3:i64 length),
void releaseVolume(1:i32 fh),

list<string> volumes(),
list<string> mountedVolumes(),


void createVolume(1:string volumeName),
void destroyVolume(1:string volumeName),
void resizeVolume(1:string volumeName, 2:i64 size)
i64 sizeVolume(1:string volumeName),

}
