<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page\Header;

class Type
{
    public const DATA_PAGE = 0;
    public const DATA_PAGE_V2 = 3;
    public const DICTIONARY_PAGE = 2;
    public const INDEX_PAGE = 1;
    public function isDataPage() : bool
    {
        return $this->value === self::DATA_PAGE->value || $this->value === self::DATA_PAGE_V2->value;
    }
    public function isDictionaryPage() : bool
    {
        return $this->value === self::DICTIONARY_PAGE->value;
    }
}
