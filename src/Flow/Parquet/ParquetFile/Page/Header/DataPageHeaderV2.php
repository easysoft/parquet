<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page\Header;

use Flow\Parquet\ParquetFile\RowGroup\StatisticsReader;
use Flow\Parquet\ParquetFile\{Encodings, Statistics};

/**
 * @psalm-suppress RedundantConditionGivenDocblockType
 * @psalm-suppress RedundantCastGivenDocblockType
 */
final class DataPageHeaderV2
{
    /**
     * @readonly
     * @var int
     */
    private $valuesCount;
    /**
     * @readonly
     * @var int
     */
    private $nullsCount;
    /**
     * @readonly
     * @var int
     */
    private $rowsCount;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Encodings
     */
    private $encoding;
    /**
     * @readonly
     * @var int
     */
    private $definitionsByteLength;
    /**
     * @readonly
     * @var int
     */
    private $repetitionsByteLength;
    /**
     * @readonly
     * @var bool|null
     */
    private $isCompressed;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Statistics|null
     */
    private $statistics;
    /**
     * @param \Flow\Parquet\ParquetFile\Encodings::* $encoding
     */
    public function __construct(int $valuesCount, int $nullsCount, int $rowsCount, $encoding, int $definitionsByteLength, int $repetitionsByteLength, ?bool $isCompressed, ?Statistics $statistics)
    {
        $this->valuesCount = $valuesCount;
        $this->nullsCount = $nullsCount;
        $this->rowsCount = $rowsCount;
        $this->encoding = $encoding;
        $this->definitionsByteLength = $definitionsByteLength;
        $this->repetitionsByteLength = $repetitionsByteLength;
        $this->isCompressed = $isCompressed;
        $this->statistics = $statistics;
    }

    public static function fromThrift(\Flow\Parquet\Thrift\DataPageHeaderV2 $thrift) : self
    {
        return new self(
            $thrift->num_values,
            $thrift->num_nulls,
            $thrift->num_rows,
            Encodings::from($thrift->encoding),
            $thrift->definition_levels_byte_length,
            $thrift->repetition_levels_byte_length,
            /** @phpstan-ignore-next-line */
            $thrift->is_compressed ?? null,
            $thrift->statistics ? Statistics::fromThrift($thrift->statistics) : null
        );
    }

    public function definitionsByteLength() : int
    {
        return $this->definitionsByteLength;
    }

    public function encoding() : Encodings
    {
        return $this->encoding;
    }

    public function repetitionsByteLength() : int
    {
        return $this->repetitionsByteLength;
    }

    public function statistics() : ?StatisticsReader
    {
        if ($this->statistics === null) {
            return null;
        }

        return new StatisticsReader($this->statistics);
    }

    public function toThrift() : \Flow\Parquet\Thrift\DataPageHeaderV2
    {
        return new \Flow\Parquet\Thrift\DataPageHeaderV2([
            'num_values' => $this->valuesCount,
            'num_nulls' => $this->nullsCount,
            'num_rows' => $this->rowsCount,
            'definition_levels_byte_length' => $this->definitionsByteLength,
            'repetition_levels_byte_length' => $this->repetitionsByteLength,
            'encoding' => $this->encoding->value,
            'is_compressed' => $this->isCompressed,
            'statistics' => ($nullsafeVariable1 = $this->statistics) ? $nullsafeVariable1->toThrift() : null,
        ]);
    }

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
