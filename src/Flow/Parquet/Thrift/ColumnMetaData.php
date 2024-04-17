<?php

declare(strict_types=1);
namespace Flow\Parquet\Thrift;

/**
 * Autogenerated by Thrift Compiler (0.19.0).
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 *  @generated
 */
use Thrift\Base\TBase;
use Thrift\Type\TType;

/**
 * Description for column metadata.
 */
class ColumnMetaData extends TBase
{
    public static $_TSPEC = [
        1 => [
            'var' => 'type',
            'isRequired' => true,
            'type' => TType::I32,
            'class' => '\Flow\Parquet\Thrift\Type',
        ],
        2 => [
            'var' => 'encodings',
            'isRequired' => true,
            'type' => TType::LST,
            'etype' => TType::I32,
            'elem' => [
                'type' => TType::I32,
                'class' => '\Flow\Parquet\Thrift\Encoding',
            ],
        ],
        3 => [
            'var' => 'path_in_schema',
            'isRequired' => true,
            'type' => TType::LST,
            'etype' => TType::STRING,
            'elem' => [
                'type' => TType::STRING,
            ],
        ],
        4 => [
            'var' => 'codec',
            'isRequired' => true,
            'type' => TType::I32,
            'class' => '\Flow\Parquet\Thrift\CompressionCodec',
        ],
        5 => [
            'var' => 'num_values',
            'isRequired' => true,
            'type' => TType::I64,
        ],
        6 => [
            'var' => 'total_uncompressed_size',
            'isRequired' => true,
            'type' => TType::I64,
        ],
        7 => [
            'var' => 'total_compressed_size',
            'isRequired' => true,
            'type' => TType::I64,
        ],
        8 => [
            'var' => 'key_value_metadata',
            'isRequired' => false,
            'type' => TType::LST,
            'etype' => TType::STRUCT,
            'elem' => [
                'type' => TType::STRUCT,
                'class' => '\Flow\Parquet\Thrift\KeyValue',
            ],
        ],
        9 => [
            'var' => 'data_page_offset',
            'isRequired' => true,
            'type' => TType::I64,
        ],
        10 => [
            'var' => 'index_page_offset',
            'isRequired' => false,
            'type' => TType::I64,
        ],
        11 => [
            'var' => 'dictionary_page_offset',
            'isRequired' => false,
            'type' => TType::I64,
        ],
        12 => [
            'var' => 'statistics',
            'isRequired' => false,
            'type' => TType::STRUCT,
            'class' => '\Flow\Parquet\Thrift\Statistics',
        ],
        13 => [
            'var' => 'encoding_stats',
            'isRequired' => false,
            'type' => TType::LST,
            'etype' => TType::STRUCT,
            'elem' => [
                'type' => TType::STRUCT,
                'class' => '\Flow\Parquet\Thrift\PageEncodingStats',
            ],
        ],
        14 => [
            'var' => 'bloom_filter_offset',
            'isRequired' => false,
            'type' => TType::I64,
        ],
        15 => [
            'var' => 'bloom_filter_length',
            'isRequired' => false,
            'type' => TType::I32,
        ],
    ];

    public static $isValidate = false;

    /**
     * Size of Bloom filter data including the serialized header, in bytes.
     * Added in 2.10 so readers may not read this field from old files and
     * it can be obtained after the BloomFilterHeader has been deserialized.
     * Writers should write this field so readers can read the bloom filter
     * in a single I/O.
     *
     * @var int
     */
    public $bloom_filter_length;

    /**
     * Byte offset from beginning of file to Bloom filter data. *.
     *
     * @var int
     */
    public $bloom_filter_offset;

    /**
     * Compression codec *.
     *
     * @var int
     */
    public $codec;

    /**
     * Byte offset from beginning of file to first data page *.
     *
     * @var int
     */
    public $data_page_offset;

    /**
     * Byte offset from the beginning of file to first (only) dictionary page *.
     *
     * @var int
     */
    public $dictionary_page_offset;

    /**
     * Set of all encodings used for pages in this column chunk.
     * This information can be used to determine if all data pages are
     * dictionary encoded for example *.
     *
     * @var PageEncodingStats[]
     */
    public $encoding_stats;

    /**
     * Set of all encodings used for this column. The purpose is to validate
     * whether we can decode those pages. *.
     *
     * @var int[]
     */
    public $encodings;

    /**
     * Byte offset from beginning of file to root index page *.
     *
     * @var int
     */
    public $index_page_offset;

    /**
     * Optional key/value metadata *.
     *
     * @var KeyValue[]
     */
    public $key_value_metadata;

    /**
     * Number of values in this column *.
     *
     * @var int
     */
    public $num_values;

    /**
     * Path in schema *.
     *
     * @var string[]
     */
    public $path_in_schema;

    /**
     * optional statistics for this column chunk.
     *
     * @var Statistics
     */
    public $statistics;

    /**
     * total byte size of all compressed, and potentially encrypted, pages
     * in this column chunk (including the headers) *.
     *
     * @var int
     */
    public $total_compressed_size;

    /**
     * total byte size of all uncompressed pages in this column chunk (including the headers) *.
     *
     * @var int
     */
    public $total_uncompressed_size;

    /**
     * Type of this column *.
     *
     * @var int
     */
    public $type;

    public function __construct($vals = null)
    {
        if (\is_array($vals)) {
            parent::__construct(self::$_TSPEC, $vals);
        }
    }

    public function getName()
    {
        return 'ColumnMetaData';
    }

    public function read($input)
    {
        return $this->_read('ColumnMetaData', self::$_TSPEC, $input);
    }

    public function write($output)
    {
        return $this->_write('ColumnMetaData', self::$_TSPEC, $output);
    }
}