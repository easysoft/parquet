<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\PlainValuesPacker;
use Flow\Parquet\ParquetFile\Page\Header\{DictionaryPageHeader, Type};
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageContainer;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\{Codec, Compressions, Encodings};
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final class DictionaryPageBuilder
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
     * @var \Flow\Parquet\Options
     */
    private $options;
    public function __construct(DataConverter $dataConverter, Compressions $compression, Options $options)
    {
        $this->dataConverter = $dataConverter;
        $this->compression = $compression;
        $this->options = $options;
    }
    public function build(FlatColumn $column, array $rows) : PageContainer
    {
        $dictionary = (new DictionaryBuilder())->build($column, $rows);

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);
        (new PlainValuesPacker($pageWriter, $this->dataConverter))->packValues($column, $dictionary->dictionary);

        $compressedBuffer = (new Codec($this->options))->compress($pageBuffer, $this->compression);

        $pageHeader = new PageHeader(Type::DICTIONARY_PAGE, \strlen($compressedBuffer), \strlen($pageBuffer), null, null, new DictionaryPageHeader(
            Encodings::PLAIN_DICTIONARY,
            \count($dictionary->dictionary)
        ));
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $compressedBuffer,
            $dictionary->indices,
            $dictionary->dictionary,
            $pageHeader
        );
    }
}
