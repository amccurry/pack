package pack.block;

public interface BlockFactory {

  Block createBlock(BlockConfig config) throws Exception;

}
