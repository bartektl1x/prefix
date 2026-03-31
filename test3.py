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


_VALID_CATALOG_PLACEHOLDERS = {"bronze_catalog", "silver_catalog", "gold_catalog"}
_PLACEHOLDER_PATTERN = re.compile(r"\{(\w+)\}")

class SourceTableConfig(ImmutableModel):
    name: NonEmptyStr
    alias: AliasName
    read_mode: ReadMode = ReadMode.BATCH

    @field_validator("name")
    @classmethod
    def _validate_catalog_placeholders(cls, value: str) -> str:
        invalid = {p for p in _PLACEHOLDER_PATTERN.findall(value) if p not in _VALID_CATALOG_PLACEHOLDERS}
        if invalid:
            raise ValueError(
                f"Invalid catalog placeholders {sorted(invalid)} in '{value}'. "
                f"Available: {sorted(_VALID_CATALOG_PLACEHOLDERS)}"
            )
        return value
        return value
