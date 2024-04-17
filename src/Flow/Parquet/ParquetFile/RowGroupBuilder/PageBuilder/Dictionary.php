<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

final class Dictionary
{
    /**
     * @var array<int, mixed>
     * @readonly
     */
    public $dictionary;
    /**
     * @var array<int, int>
     * @readonly
     */
    public $indices;
    /**
     * @param array<int, mixed> $dictionary
     * @param array<int, int> $indices
     */
    public function __construct(array $dictionary, array $indices)
    {
        $this->dictionary = $dictionary;
        $this->indices = $indices;
    }
}
