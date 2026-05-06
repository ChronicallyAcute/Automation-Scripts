"""
Generate synthetic GeneFisher training data.

Produces:
  test_data/genomes/  — three organism genome .fna files
  test_data/queries/  — one multi-gene query FASTA

Each query gene is embedded into the genomes at three identity tiers:
  genome_alpha  — 100 %  (exact)
  genome_beta   — ~90 %  (same genus, different species)
  genome_gamma  — ~75 %  (distantly related organism)

Flanking random sequence fills out each contig to realistic chromosome-
fragment lengths (~120–200 kb per file).
"""

import random
from pathlib import Path

random.seed(42)

OUT_GENOMES = Path("test_data/genomes")
OUT_QUERIES = Path("test_data/queries")

COMPLEMENT = str.maketrans("ACGTacgt", "TGCAtgca")

# ── sequence helpers ──────────────────────────────────────────────────────────

def rand_seq(length: int, gc: float = 0.50) -> str:
    at = (1 - gc) / 2
    weights = [gc / 2, at, at, gc / 2]          # G A T C
    return "".join(random.choices("GATC", weights=weights, k=length))


def mutate(seq: str, target_pct: float) -> str:
    """Return seq with approximately (100 - target_pct) % positions substituted."""
    bases = list(seq)
    n_mut = int(len(seq) * (1 - target_pct / 100))
    positions = random.sample(range(len(seq)), n_mut)
    for i in positions:
        orig = bases[i].upper()
        choices = [b for b in "ACGT" if b != orig]
        bases[i] = random.choice(choices)
    return "".join(bases)


def wrap(seq: str, width: int = 60) -> str:
    return "\n".join(seq[i:i+width] for i in range(0, len(seq), width))


def revcomp(seq: str) -> str:
    return seq.translate(COMPLEMENT)[::-1]


# ── gene definitions ──────────────────────────────────────────────────────────
# Four synthetic marker genes with biologically plausible lengths and GC content.

GENES = {
    "rpoB": {
        "desc": "RNA polymerase beta subunit (partial)",
        "length": 1200,
        "gc": 0.55,
    },
    "gyrB": {
        "desc": "DNA gyrase subunit B (partial)",
        "length": 900,
        "gc": 0.52,
    },
    "recA": {
        "desc": "Recombinase A",
        "length": 750,
        "gc": 0.48,
    },
    "rrs": {
        "desc": "16S ribosomal RNA gene (partial)",
        "length": 1500,
        "gc": 0.54,
    },
}

# Generate canonical (reference) sequences for each gene
gene_seqs = {name: rand_seq(info["length"], info["gc"])
             for name, info in GENES.items()}

# ── genome builder ────────────────────────────────────────────────────────────

def build_genome(
    organism: str,
    strain: str,
    accession: str,
    n_contigs: int,
    contig_size: int,
    identity_pct: float,
    gc: float,
) -> list[tuple[str, str]]:
    """
    Return a list of (header, sequence) tuples.

    One contig per gene has the gene embedded at the requested identity.
    Remaining contigs are pure random sequence.
    """
    records = []
    gene_items = list(gene_seqs.items())

    for i in range(n_contigs):
        header = (
            f"{accession}.{i+1} {organism} {strain} "
            f"contig_{i+1:03d}, whole genome shotgun sequence"
        )

        if i < len(gene_items):
            gene_name, ref_seq = gene_items[i]
            # mutate to target identity
            embedded = mutate(ref_seq, identity_pct)
            # randomly complement half the time (tests strand handling)
            if random.random() < 0.5:
                embedded = revcomp(embedded)

            flank_total = contig_size - len(embedded)
            left  = rand_seq(flank_total // 2, gc)
            right = rand_seq(flank_total - flank_total // 2, gc)
            seq = left + embedded + right
        else:
            seq = rand_seq(contig_size, gc)

        records.append((header, seq))

    return records


def write_fasta(path: Path, records: list[tuple[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for header, seq in records:
            fh.write(f">{header}\n{wrap(seq)}\n")
    print(f"  wrote {path}  ({len(records)} contig(s), "
          f"{sum(len(s) for _, s in records):,} bp)")


# ── genome alpha — exact match (100 %) ───────────────────────────────────────

alpha = build_genome(
    organism="Pseudomonas aeruginosa",
    strain="PA_ALPHA_01",
    accession="GF_ALPHA",
    n_contigs=8,
    contig_size=25_000,
    identity_pct=100.0,
    gc=0.66,
)
write_fasta(OUT_GENOMES / "genome_alpha.fna", alpha)

# ── genome beta — ~90 % identity (same genus, different species) ─────────────

beta = build_genome(
    organism="Pseudomonas fluorescens",
    strain="PF_BETA_04",
    accession="GF_BETA",
    n_contigs=6,
    contig_size=30_000,
    identity_pct=90.0,
    gc=0.60,
)
write_fasta(OUT_GENOMES / "genome_beta.fna", beta)

# ── genome gamma — ~75 % identity (distantly related) ────────────────────────

gamma = build_genome(
    organism="Burkholderia cepacia",
    strain="BC_GAMMA_07",
    accession="GF_GAMMA",
    n_contigs=10,
    contig_size=20_000,
    identity_pct=75.0,
    gc=0.68,
)
write_fasta(OUT_GENOMES / "genome_gamma.fna", gamma)

# ── query FASTA ───────────────────────────────────────────────────────────────

query_path = OUT_QUERIES / "query_genes.fasta"
query_path.parent.mkdir(parents=True, exist_ok=True)
with query_path.open("w") as fh:
    for name, info in GENES.items():
        fh.write(f">{name} {info['desc']}\n{wrap(gene_seqs[name])}\n")
print(f"  wrote {query_path}  ({len(GENES)} query gene(s))")

# ── summary ───────────────────────────────────────────────────────────────────

print()
print("Test data summary")
print("─" * 52)
print("Genomes (place in a single folder for Step 1):")
for f in sorted(OUT_GENOMES.glob("*.fna")):
    size_kb = f.stat().st_size / 1024
    print(f"  {f.name:<26}  {size_kb:7.1f} KB")
print()
print("Query file (use in Step 2):")
print(f"  {query_path}")
print()
print("Expected BLAST hits per genome:")
print(f"  genome_alpha  — 4 hits @ ~100 % identity  (green rows)")
print(f"  genome_beta   — 4 hits @ ~ 90 % identity  (yellow rows)")
print(f"  genome_gamma  — 4 hits @ ~ 75 % identity  (orange rows)")
print()
print("Done.")
