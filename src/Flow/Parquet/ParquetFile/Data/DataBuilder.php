<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Dremel\Dremel;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Page\ColumnData;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class DataBuilder
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

    public function build(ColumnData $columnData, FlatColumn $column) : \Generator
    {
        $dremel = new Dremel();

        foreach ($dremel->assemble($columnData->repetitions, $columnData->definitions, $columnData->values) as $value) {
            yield $this->enrichData($value, $column);
        }
    }

    /**
     * @param mixed $value
     * @return mixed
     */
    private function enrichData($value, FlatColumn $column)
    {
        if ($value === null) {
            return null;
        }

        if (\is_array($value)) {
            $enriched = [];

            foreach ($value as $val) {
                $enriched[] = $this->dataConverter->fromParquetType($column, $val);
            }

            return $enriched;
        }

        return $this->dataConverter->fromParquetType($column, $value);
    }
}
