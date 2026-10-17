# CAP-SINGLE-POD-L2-R1 Design Premerge Gate

- R1 frozen Evidence base: `d1899ecab8d71248062ffd70eb4e42048e394a38`
- Reviewed Design Head: `a591ec6c3c76878d72e00fff3be7f9a6f379a4de`
- Review result: `25ba9675fbab1a1ea796e39a52fefcc16de351c3`
- Premerge candidate: `93d86bce4b8c9058eb89ab464ba1a5b981da751d`

```text
go test ./... -count=1        PASS
go vet ./...                   PASS
go build ./cmd/...             PASS
JSON / JSON Schema             PASS
exact design hash index        PASS
scope / secret review          PASS
```

Result: `PREMERGE_GATE=PASS`. Harness implementation is authorized only in Train-A paths. Smoke, pressure, Mixed and capacity claims remain blocked.
