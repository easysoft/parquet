<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\Page\PageHeader;

final class PageContainer
{
    /**
     * @var string
     * @readonly
     */
    public $pageHeaderBuffer;
    /**
     * @var string
     * @readonly
     */
    public $pageBuffer;
    /**
     * @var array
     * @readonly
     */
    public $values;
    /**
     * @var null|array
     * @readonly
     */
    public $dictionary;
    /**
     * @var PageHeader
     * @readonly
     */
    public $pageHeader;
    /**
     * @param string $pageHeaderBuffer
     * @param string $pageBuffer
     * @param array $values - when dictionary is present values are indices
     * @param null|array $dictionary
     * @param PageHeader $pageHeader
     */
    public function __construct(string $pageHeaderBuffer, string $pageBuffer, array $values, ?array $dictionary, PageHeader $pageHeader)
    {
        $this->pageHeaderBuffer = $pageHeaderBuffer;
        $this->pageBuffer = $pageBuffer;
        $this->values = $values;
        $this->dictionary = $dictionary;
        $this->pageHeader = $pageHeader;
    }

    public function dataSize() : int
    {
        return \strlen($this->pageBuffer);
    }

    public function headerSize() : int
    {
        return \strlen($this->pageHeaderBuffer);
    }

    public function totalCompressedSize() : int
    {
        return $this->headerSize() + $this->pageHeader->compressedPageSize();
    }

    public function totalUncompressedSize() : int
    {
        return $this->headerSize() + $this->pageHeader->uncompressedPageSize();
    }
}
