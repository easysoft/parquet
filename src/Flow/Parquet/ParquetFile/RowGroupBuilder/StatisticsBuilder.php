<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Data\PlainValuesPacker;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

final class StatisticsBuilder
{
    /**
     * @readonly
     * @var \Flow\Parquet\Data\DataConverter
     */
    private $dataConverter;
    public function __construct(DataConverter $dataConverter)
    {
        $this->dataConverter = $dataConverter;
    }

    public function build(FlatColumn $column, ColumnChunkStatistics $chunkStatistics) : Statistics
    {
        $minBuffer = '';
        $maxBuffer = '';

        (new PlainValuesPacker(new BinaryBufferWriter($minBuffer), $this->dataConverter))->packValues($column, [$chunkStatistics->min()]);
        (new PlainValuesPacker(new BinaryBufferWriter($maxBuffer), $this->dataConverter))->packValues($column, [$chunkStatistics->max()]);

        return new Statistics($maxBuffer, $minBuffer, $chunkStatistics->nullCount(), $chunkStatistics->distinctCount(), $maxBuffer, $minBuffer);
    }
}
