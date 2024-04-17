<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;

final class RowGroup
{
    /**
     * @var array<ColumnChunk>
     */
    private $columnChunks;
    /**
     * @var int
     */
    private $rowsCount;
    /**
     * @param array<ColumnChunk> $columnChunks
     * @param int $rowsCount
     */
    public function __construct(array $columnChunks, int $rowsCount)
    {
        $this->columnChunks = $columnChunks;
        $this->rowsCount = $rowsCount;
    }
    public static function fromThrift(\Flow\Parquet\Thrift\RowGroup $thrift) : self
    {
        return new self(
            \array_map(static function (\Flow\Parquet\Thrift\ColumnChunk $columnChunk) {
                return ColumnChunk::fromThrift($columnChunk);
            }, $thrift->columns),
            $thrift->num_rows
        );
    }

    public function addColumnChunk(ColumnChunk $columnChunk) : void
    {
        $this->columnChunks[] = $columnChunk;
    }

    /**
     * @return array<ColumnChunk>
     */
    public function columnChunks() : array
    {
        return $this->columnChunks;
    }

    public function rowsCount() : int
    {
        return $this->rowsCount;
    }

    public function setRowsCount(int $rowsCount) : void
    {
        if ($rowsCount < 0) {
            throw new InvalidArgumentException('Rows count must be greater than 0');
        }

        $this->rowsCount = $rowsCount;
    }

    public function totalByteSize() : int
    {
        return \array_sum(\array_map(static function (ColumnChunk $chunk) {
            return $chunk->totalUncompressedSize();
        }, $this->columnChunks));
    }

    public function toThrift() : \Flow\Parquet\Thrift\RowGroup
    {
        $fileOffset = \count($this->columnChunks) ? \current($this->columnChunks)->fileOffset() : 0;
        $chunksUncompressedSize = \array_map(static function (ColumnChunk $chunk) {
            return $chunk->totalUncompressedSize();
        }, $this->columnChunks);
        $chunksCompressedSize = \array_map(static function (ColumnChunk $chunk) {
            return $chunk->totalCompressedSize();
        }, $this->columnChunks);

        return new \Flow\Parquet\Thrift\RowGroup([
            'columns' => \array_map(static function (ColumnChunk $columnChunk) {
                return $columnChunk->toThrift();
            }, $this->columnChunks),
            'num_rows' => $this->rowsCount,
            'file_offset' => $fileOffset,
            'total_byte_size' => \array_sum($chunksUncompressedSize),
            'total_compressed_size' => \array_sum($chunksCompressedSize),
        ]);
    }
}
