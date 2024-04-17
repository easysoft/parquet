<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\{Option, Options};

final class Codec
{
    /**
     * @readonly
     * @var \Flow\Parquet\Options
     */
    private $options;
    public function __construct(Options $options)
    {
        $this->options = $options;
    }

    public function compress(string $data, Compressions $compression) : string
    {
        switch ($compression) {
            case Compressions::UNCOMPRESSED:
                $result = $data;
                break;
            case Compressions::SNAPPY:
                $result = \snappy_compress($data);
                break;
            case Compressions::GZIP:
                $result = \gzencode($data, $this->options->get(Option::GZIP_COMPRESSION_LEVEL));
                break;
            default:
                throw new RuntimeException('Compression ' . $compression->name . ' is not supported yet');
        }

        if ($result === false) {
            throw new RuntimeException('Failed to decompress data');
        }

        return $result;
    }

    public function decompress(string $data, Compressions $compression) : string
    {
        switch ($compression) {
            case Compressions::UNCOMPRESSED:
                $result = $data;
                break;
            case Compressions::SNAPPY:
                $result = \snappy_uncompress($data);
                break;
            case Compressions::GZIP:
                $result = \gzdecode($data);
                break;
            default:
                throw new RuntimeException('Compression ' . $compression->name . ' is not supported yet');
        }

        if ($result === false) {
            throw new RuntimeException('Failed to decompress data');
        }

        return $result;
    }
}
