<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\Validator;

use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator;
use Flow\Parquet\ParquetFile\Schema\Column;

final class DisabledValidator implements Validator
{
    /**
     * @param mixed $data
     */
    public function validate(Column $column, $data) : void
    {
    }
}
