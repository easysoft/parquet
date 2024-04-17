<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\ParquetFile\Schema\Column;

interface Validator
{
    /**
     * @throws ValidationException
     * @param mixed $data
     */
    public function validate(Column $column, $data) : void;
}
