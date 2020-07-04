# tt-extractor-kindle

Extracts contents from Kindle Clippings into ThoughtTrain Common Table Format.

## Installation

```
go get -u github.com/milanaleksic/tt_extractor_kindle/cmd/tt-extractor-kindle
```

## Usage

```
$ tt-extractor-kindle -help
Usage of ./tt-extractor-kindle:
  -database string
        SQLite3 database location (default "clippings.db")
  -debug
        show debug messages
  -input-file value
        input clipping files
```

## Example

I have had 2 Kindles, so I have 2 clipping files, let me ingest both of them
into a single sqlite DB (default `clippings.db`).

```
tt-extractor-kindle \
  -input-file clippings.txt \
  -input-file old-clippings.txt
```
