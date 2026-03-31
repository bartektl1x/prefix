SourceTableReference = Annotated[
    str,
    StringConstraints(
        min_length=1,
        strip_whitespace=True,
        pattern=r"^(\{[a-zA-Z_]\w*\}|[a-zA-Z_]\w*)(\.[a-zA-Z_]\w*)*$",
    ),
]



def _resolve_table_name(self, name: str) -> str:
    try:
        return name.format(
            bronze_catalog=self.bronze_catalog,
            silver_catalog=self.silver_catalog,
            gold_catalog=self.gold_catalog,
        )
    except KeyError as exc:
        exc.add_note(
            f"Unresolvable catalog placeholder {exc} in '{name}'. "
            f"Available placeholders: {{bronze_catalog}}, {{silver_catalog}}, {{gold_catalog}}"
        )
        raise
