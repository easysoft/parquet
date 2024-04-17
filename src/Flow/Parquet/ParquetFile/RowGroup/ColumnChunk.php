<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\{Compressions, Encodings, Statistics};
use Flow\Parquet\Thrift\ColumnMetaData;

final class ColumnChunk
{
    /**
     * @var PhysicalType
     * @readonly
     */
    private $type;
    /**
     * @var Compressions
     * @readonly
     */
    private $codec;
    /**
     * @var int
     * @readonly
     */
    private $valuesCount;
    /**
     * @var int
     * @readonly
     */
    private $fileOffset;
    /**
     * @var array<string>
     * @readonly
     */
    private $path;
    /**
     * @var array<Encodings>
     * @readonly
     */
    private $encodings;
    /**
     * @var int
     * @readonly
     */
    private $totalCompressedSize;
    /**
     * @var int
     * @readonly
     */
    private $totalUncompressedSize;
    /**
     * @var null|int
     * @readonly
     */
    private $dictionaryPageOffset;
    /**
     * @var null|int
     * @readonly
     */
    private $dataPageOffset;
    /**
     * @var null|int
     * @readonly
     */
    private $indexPageOffset;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Statistics|null
     */
    private $statistics;
    /**
     * @param PhysicalType $type
     * @param Compressions $codec
     * @param int $valuesCount
     * @param int $fileOffset
     * @param array<string> $path
     * @param array<Encodings> $encodings
     * @param int $totalCompressedSize
     * @param int $totalUncompressedSize
     * @param null|int $dictionaryPageOffset
     * @param null|int $dataPageOffset
     * @param null|int $indexPageOffset
     */
    public function __construct(PhysicalType $type, Compressions $codec, int $valuesCount, int $fileOffset, array $path, array $encodings, int $totalCompressedSize, int $totalUncompressedSize, ?int $dictionaryPageOffset, ?int $dataPageOffset, ?int $indexPageOffset, ?Statistics $statistics)
    {
        $this->type = $type;
        $this->codec = $codec;
        $this->valuesCount = $valuesCount;
        $this->fileOffset = $fileOffset;
        $this->path = $path;
        $this->encodings = $encodings;
        $this->totalCompressedSize = $totalCompressedSize;
        $this->totalUncompressedSize = $totalUncompressedSize;
        $this->dictionaryPageOffset = $dictionaryPageOffset;
        $this->dataPageOffset = $dataPageOffset;
        $this->indexPageOffset = $indexPageOffset;
        $this->statistics = $statistics;
    }
    /**
     * @psalm-suppress RedundantConditionGivenDocblockType
     */
    public static function fromThrift(\Flow\Parquet\Thrift\ColumnChunk $thrift) : self
    {
        return new self(PhysicalType::from($thrift->meta_data->type), Compressions::from($thrift->meta_data->codec), $thrift->meta_data->num_values, $thrift->file_offset, $thrift->meta_data->path_in_schema, \array_map(static function ($encoding) {
            return Encodings::from($encoding);
        }, $thrift->meta_data->encodings), $thrift->meta_data->total_compressed_size, $thrift->meta_data->total_uncompressed_size, $thrift->meta_data->dictionary_page_offset, $thrift->meta_data->data_page_offset, $thrift->meta_data->index_page_offset, $thrift->meta_data->statistics ? Statistics::fromThrift($thrift->meta_data->statistics) : null);
    }

    public function codec() : Compressions
    {
        return $this->codec;
    }

    public function dataPageOffset() : ?int
    {
        return $this->dataPageOffset;
    }

    public function dictionaryPageOffset() : ?int
    {
        return $this->dictionaryPageOffset;
    }

    /**
     * @return array<Encodings>
     */
    public function encodings() : array
    {
        return $this->encodings;
    }

    public function fileOffset() : int
    {
        return $this->fileOffset;
    }

    public function flatPath() : string
    {
        return \implode('.', $this->path);
    }

    /**
     * @psalm-suppress ArgumentTypeCoercion
     */
    public function pageOffset() : int
    {
        $offset = \min(
            \array_filter([
                $this->dictionaryPageOffset,
                $this->dataPageOffset,
                $this->indexPageOffset,
            ])
        );

        return $offset;
    }

    public function statistics() : ?StatisticsReader
    {
        if ($this->statistics === null) {
            return null;
        }

        return new StatisticsReader($this->statistics);
    }

    public function totalCompressedSize() : int
    {
        return $this->totalCompressedSize;
    }

    public function totalUncompressedSize() : int
    {
        return $this->totalUncompressedSize;
    }

    public function toThrift() : \Flow\Parquet\Thrift\ColumnChunk
    {
        return new \Flow\Parquet\Thrift\ColumnChunk([
            'file_offset' => $this->fileOffset,
            'meta_data' => new ColumnMetaData([
                'type' => $this->type->value,
                'encodings' => \array_map(static function (Encodings $encoding) {
                    return $encoding->value;
                }, $this->encodings),
                'path_in_schema' => $this->path,
                'codec' => $this->codec->value,
                'num_values' => $this->valuesCount,
                'total_uncompressed_size' => $this->totalUncompressedSize,
                'total_compressed_size' => $this->totalCompressedSize,
                'data_page_offset' => $this->dataPageOffset,
                'index_page_offset' => $this->indexPageOffset,
                'dictionary_page_offset' => $this->dictionaryPageOffset,
                'statistics' => ($nullsafeVariable1 = $this->statistics) ? $nullsafeVariable1->toThrift() : null,
            ]),
        ]);
    }

    public function type() : PhysicalType
    {
        return $this->type;
    }

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
