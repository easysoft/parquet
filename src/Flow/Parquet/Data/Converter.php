<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface Converter
{
    /**
     * @param mixed $data
     * @return mixed
     */
    public function fromParquetType($data);

    public function isFor(FlatColumn $column, Options $options) : bool;

    /**
     * @param mixed $data
     * @return mixed
     */
    public function toParquetType($data);
}
