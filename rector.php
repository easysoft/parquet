<?php

use Rector\Config\RectorConfig;
use Rector\Set\ValueObject\DowngradeLevelSetList;
use Rector\ValueObject\PhpVersion;
use Rector\Caching\ValueObject\Storage\FileCacheStorage;

$version = getenv('PHP_VERSION');
$version = $version ?: '72';
$cacheDir = getenv('RECTOR_CACHE_DIR');
$cacheDir = $cacheDir ?: '/tmp/rector_cached_files';

return static function (RectorConfig $rectorConfig) use ($version, $cacheDir): void {
    $rectorConfig->phpVersion(constant(PhpVersion::class . '::PHP_' . $version));
    $rectorConfig->sets([constant(DowngradeLevelSetList::class . '::DOWN_TO_PHP_' . $version)]);

    $rectorConfig->cacheClass(FileCacheStorage::class);
    $rectorConfig->cacheDirectory($cacheDir);
};
