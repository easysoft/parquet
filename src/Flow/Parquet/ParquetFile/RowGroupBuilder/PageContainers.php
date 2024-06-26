<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\Type;

final class PageContainers
{
    /**
     * @var array<PageContainer>
     */
    private $dataPageContainers = [];

    /**
     * @var \Flow\Parquet\ParquetFile\RowGroupBuilder\PageContainer|null
     */
    private $dictionaryPageContainer;

    public function __construct(array $containers = [])
    {
        foreach ($containers as $container) {
            $this->add($container);
        }
    }

    public function add(PageContainer $container) : void
    {
        if ($container->pageHeader->type() === Type::DICTIONARY_PAGE) {
            if ($this->dictionaryPageContainer !== null) {
                throw new InvalidArgumentException('Dictionary page container already set');
            }

            $this->dictionaryPageContainer = $container;

            return;
        }

        $this->dataPageContainers[] = $container;
    }

    public function buffer() : string
    {
        $buffer = '';

        if ($this->dictionaryPageContainer) {
            $buffer .= $this->dictionaryPageContainer->pageHeaderBuffer;
            $buffer .= $this->dictionaryPageContainer->pageBuffer;
        }

        foreach ($this->dataPageContainers as $pageContainer) {
            $buffer .= $pageContainer->pageHeaderBuffer;
            $buffer .= $pageContainer->pageBuffer;
        }

        return $buffer;
    }

    public function compressedSize() : int
    {
        $size = 0;

        if ($this->dictionaryPageContainer) {
            $size += $this->dictionaryPageContainer->totalCompressedSize();
        }

        foreach ($this->dataPageContainers as $pageContainer) {
            $size += $pageContainer->totalCompressedSize();
        }

        return $size;
    }

    public function dataPageContainers() : array
    {
        return $this->dataPageContainers;
    }

    public function dictionaryPageContainer() : ?PageContainer
    {
        return $this->dictionaryPageContainer;
    }

    /**
     * @return array<Encodings>
     */
    public function encodings() : array
    {
        $encodings = [];

        if ($this->dictionaryPageContainer) {
            $encodings[] = $this->dictionaryPageContainer->pageHeader->encoding()->value;
        }

        foreach ($this->dataPageContainers as $pageContainer) {
            if ($pageContainer->pageHeader->dataPageHeader()) {
                $encodings[] = $pageContainer->pageHeader->dataPageHeader()->repetitionLevelEncoding()->value;
                $encodings[] = $pageContainer->pageHeader->dataPageHeader()->definitionLevelEncoding()->value;
                $encodings[] = $pageContainer->pageHeader->dataPageHeader()->encoding()->value;
            }

            if ($pageContainer->pageHeader->dataPageHeaderV2()) {
                $encodings[] = $pageContainer->pageHeader->dataPageHeaderV2()->encoding()->value;
            }
        }

        $encodings = \array_unique($encodings);

        return \array_map(static function (int $encoding) {
            return Encodings::from($encoding);
        }, $encodings);
    }

    public function uncompressedSize() : int
    {
        $size = 0;

        if ($this->dictionaryPageContainer) {
            $size += $this->dictionaryPageContainer->totalUncompressedSize();
        }

        foreach ($this->dataPageContainers as $pageContainer) {
            $size += $pageContainer->totalUncompressedSize();
        }

        return $size;
    }

    public function valuesCount() : int
    {
        $count = 0;

        foreach ($this->dataPageContainers as $pageContainer) {
            $count += $pageContainer->pageHeader->dataValuesCount();
        }

        return $count;
    }
}
