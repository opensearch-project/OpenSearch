# Tamil Analysis Plugin for OpenSearch

This plugin provides Tamil language analysis components for OpenSearch, enabling better search relevance for Tamil text by handling morphological variations and filtering common stopwords.

## Components

The plugin registers three analysis components:

| Component | Type | Description |
|-----------|------|-------------|
| `tamil_stemmer` | Token Filter | Suffix-stripping stemmer for Tamil inflected forms |
| `tamil_stop` | Token Filter | Stopword filter with bundled Tamil stopword set |
| `tamil` | Analyzer | Prebuilt analyzer combining the above filters |

## Installation

```bash
bin/opensearch-plugin install file:///path/to/analysis-tamil-<version>.zip
```

Restart OpenSearch after installation. For cluster deployments, install on every node.

## Usage

### Using the Prebuilt Analyzer

The simplest usage is the prebuilt `tamil` analyzer:

```json
PUT /my-index
{
  "mappings": {
    "properties": {
      "content": {
        "type": "text",
        "analyzer": "tamil"
      }
    }
  }
}
```

### Custom Analyzer with ICU (Recommended for Production)

For better Unicode handling and tokenization, compose a custom analyzer using components from both `analysis-icu` and `analysis-tamil`:

```json
PUT /my-index
{
  "settings": {
    "analysis": {
      "char_filter": {
        "tamil_nfc": {
          "type": "icu_normalizer",
          "name": "nfc",
          "mode": "compose"
        }
      },
      "filter": {
        "tamil_stop_filter": { "type": "tamil_stop" },
        "tamil_stem_filter": { "type": "tamil_stemmer", "min_stem_length": 3 }
      },
      "analyzer": {
        "tamil_custom": {
          "type": "custom",
          "char_filter": ["tamil_nfc"],
          "tokenizer": "icu_tokenizer",
          "filter": ["lowercase", "tamil_stop_filter", "tamil_stem_filter"]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "content": {
        "type": "text",
        "analyzer": "tamil_custom"
      }
    }
  }
}
```

### Using Individual Filters

#### Tamil Stemmer

Strips common Tamil suffixes (case markers, plural, postpositions):

```json
PUT /my-index
{
  "settings": {
    "analysis": {
      "filter": {
        "my_tamil_stemmer": {
          "type": "tamil_stemmer",
          "min_stem_length": 3
        }
      }
    }
  }
}
```

**Parameters:**
- `min_stem_length` (integer, default: 3) - Minimum length a stem must have after suffix removal. Prevents over-stemming short words.

**Examples:**
| Input | Output | Suffix Stripped |
|-------|--------|-----------------|
| பள்ளிக்கு | பள்ளி | க்கு (dative) |
| குழந்தைகள் | குழந்தை | கள் (plural) |
| வீட்டில் | வீட்ட | ில் (locative) |
| மாணவர்களுக்கு | மாணவர் | களுக்கு (plural+dative) |

#### Tamil Stop Filter

Removes common Tamil stopwords:

```json
PUT /my-index
{
  "settings": {
    "analysis": {
      "filter": {
        "my_tamil_stop": {
          "type": "tamil_stop",
          "stopwords": ["நான்", "நீ", "அவன்"]
        }
      }
    }
  }
}
```

**Parameters:**
- `stopwords` (array) - Custom list of stopwords (replaces default list)
- `stopwords_path` (string) - Path to a file containing stopwords (one per line)

If neither parameter is specified, uses the bundled default stopword set which includes:
- Personal pronouns (நான், நீ, அவன், அவள், etc.)
- Demonstratives (இந்த, அந்த, எந்த, etc.)
- Auxiliary verbs (இருக்கிறது, இல்லை, உள்ளது, etc.)
- Common conjunctions (மற்றும், அல்லது, ஆனால், etc.)
- Particles and markers (தான், மட்டும், கூட, etc.)

### Testing with Analyze API

```json
POST /_analyze
{
  "analyzer": "tamil",
  "text": "நான் பள்ளிக்கு போனேன்"
}
```

Expected output: `["பள்ளி", "போனேன்"]` (stopword "நான்" removed, "பள்ளிக்கு" stemmed)

## How the Stemmer Works

The Tamil stemmer uses ordered suffix stripping with these characteristics:

1. **Longest-first matching**: Longer suffixes are checked before shorter ones to handle compound suffixes correctly (e.g., `களுக்கு` before `க்கு`)

2. **Minimum stem length guard**: Prevents stripping suffixes that would result in stems shorter than `min_stem_length`

3. **Single-pass stripping**: Only one suffix is removed per token per pass

### Supported Suffixes

The stemmer handles common Tamil inflectional patterns:

- **Case markers**: க்கு (dative), ில் (locative), ின் (genitive), ால் (instrumental)
- **Plural combinations**: களுக்கு, களில், களின், களால், களை
- **Plural marker**: கள்
- **Accusative**: யை (after vowels)
- **Postpositions**: உடன், வரை, போல், இடம்

## Limitations

1. **Suffix stripping, not morphological analysis**: This stemmer uses rule-based suffix stripping, not a full morphological analyzer. It may under-strip rare suffixes or occasionally over-strip.

2. **Sandhi not handled**: Tamil sandhi (morphophonemic changes at word boundaries) is not resolved. For better handling, use `icu_tokenizer`.

3. **Stopwords are task-dependent**: The bundled stopword list is a reasonable starting point but may need customization for specific use cases.

4. **Reindexing required**: Changes to analyzer configuration require reindexing. Use index aliases to swap indices without downtime.

## Version Compatibility

This plugin is version-locked to OpenSearch. Each OpenSearch upgrade requires a matching plugin version.

| Plugin Version | OpenSearch Version |
|----------------|-------------------|
| 3.7.0-SNAPSHOT | 3.7.0 |

## Building from Source

```bash
cd OpenSearch
./gradlew :plugins:analysis-tamil:assemble
```

The plugin zip will be at `plugins/analysis-tamil/build/distributions/analysis-tamil-<version>.zip`

## License

Apache License 2.0

## References

- [OpenSearch Analysis Documentation](https://opensearch.org/docs/latest/analyzers/)
- [ICU Analysis Plugin](https://opensearch.org/docs/latest/analyzers/language-analyzers/icu/)
