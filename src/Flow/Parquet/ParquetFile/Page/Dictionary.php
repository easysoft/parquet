<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page;

final class Dictionary
{
    /**
     * @var mixed[]
     */
    public $values;
    public function __construct(array $values)
    {
        $this->values = $values;
    }
}
