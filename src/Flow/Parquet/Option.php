<?php

declare(strict_types=1);

namespace Flow\Parquet;

class Option
{
    public const BYTE_ARRAY_TO_STRING = 'byte_array_to_string';
    public const DICTIONARY_PAGE_MIN_CARDINALITY_RATION = 'dictionary_page_min_cardinality_ration';
    public const DICTIONARY_PAGE_SIZE = 'dictionary_page_size';
    public const GZIP_COMPRESSION_LEVEL = 'gzip_compression_level';
    public const INT_96_AS_DATETIME = 'int_96_as_datetime';
    public const PAGE_SIZE_BYTES = 'page_size_bytes';
    public const ROUND_NANOSECONDS = 'round_nanoseconds';
    public const ROW_GROUP_SIZE_BYTES = 'row_group_size_bytes';
    public const ROW_GROUP_SIZE_CHECK_INTERVAL = 'row_group_size_check_interval';
    public const VALIDATE_DATA = 'validate_data';
    public const WRITER_VERSION = 'writer_version';
}
