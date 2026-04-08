# bgee-rdf

Bgee gene expression RDF dump as a BioBrick.

[Bgee](https://bgee.org/) ("Web of Biological Data") is a database of gene
expression patterns in animals. It integrates curated data from RNA-Seq,
microarray, in-situ hybridization, and EST experiments across dozens of
species, providing a reference atlas of where genes are expressed.

## Contents

The download contains the full Bgee RDF dump in Turtle format:
- Gene expression calls (present/absent) per tissue + developmental stage
- Links to Uberon anatomy ontology, Gene Ontology, NCBI taxonomy
- Comparative gene expression across human, mouse, rat, zebrafish, and more

Compressed size: ~2-5 GB

## SPARQL Endpoint

Official: https://bgee.org/sparql

## Usage

```bash
# Download RDF dump
uv run python stages/01_download.py

# Via DVC
dvc repro
```

## Schema

Bgee RDF uses the `genex:` namespace.
Key properties: `genex:isExpressedIn`, `genex:hasExpressionCondition`,
`genex:hasAnatomicalEntity` (Uberon terms), `genex:hasDevelopmentalStage`.
