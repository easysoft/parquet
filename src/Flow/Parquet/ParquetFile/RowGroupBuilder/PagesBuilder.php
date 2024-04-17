<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\{DataPageBuilder, DictionaryPageBuilder};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};
use Flow\Parquet\{Option, Options};

final class PagesBuilder
{
    /**
     * @readonly
     * @var \Flow\Parquet\Data\DataConverter
     */
    private $dataConverter;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Compressions
     */
    private $compression;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\RowGroupBuilder\PageSizeCalculator
     */
    private $pageSizeCalculator;
    /**
     * @readonly
     * @var \Flow\Parquet\Options
     */
    private $options;
    public function __construct(DataConverter $dataConverter, Compressions $compression, PageSizeCalculator $pageSizeCalculator, Options $options)
    {
        $this->dataConverter = $dataConverter;
        $this->compression = $compression;
        $this->pageSizeCalculator = $pageSizeCalculator;
        $this->options = $options;
    }

    public function build(FlatColumn $column, array $rows, ColumnChunkStatistics $statistics) : PageContainers
    {
        $containers = new PageContainers();

        if ($column->type() !== PhysicalType::BOOLEAN) {
            if ($statistics->cardinalityRation() <= $this->options->get(Option::DICTIONARY_PAGE_MIN_CARDINALITY_RATION)) {
                $dictionaryPageContainer = (new DictionaryPageBuilder($this->dataConverter, $this->compression, $this->options))->build($column, $rows);

                if ($dictionaryPageContainer->dataSize() <= $this->options->get(Option::DICTIONARY_PAGE_SIZE)) {
                    $containers->add($dictionaryPageContainer);

                    $containers->add(
                        (new DataPageBuilder($this->dataConverter, $this->compression, $this->options))->build($column, $rows, $dictionaryPageContainer->dictionary, $dictionaryPageContainer->values)
                    );

                    return $containers;
                }
                $dictionaryPageContainer = null;
            }
        }

        /* @phpstan-ignore-next-line */
        foreach (\array_chunk($rows, $this->pageSizeCalculator->rowsPerPage($column, $statistics)) as $rowsChunk) {
            $containers->add((new DataPageBuilder($this->dataConverter, $this->compression, $this->options))->build($column, $rowsChunk));
        }

        return $containers;
    }
}
