-- Reconstructed illustrative snapshots, independent of the entity lookup.
CREATE VIEW main.atlas_effectifs AS
SELECT entite, mois_snapshot, effectif
FROM main.snapshots_effectifs;
