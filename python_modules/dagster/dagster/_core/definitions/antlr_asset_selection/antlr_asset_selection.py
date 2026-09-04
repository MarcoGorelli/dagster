from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener

from dagster._core.definitions.antlr_asset_selection.generated.AssetSelectionLexer import (
    AssetSelectionLexer,
)
from dagster._core.definitions.antlr_asset_selection.generated.AssetSelectionParser import (
    AssetSelectionParser,
)
from dagster._core.definitions.antlr_asset_selection.generated.AssetSelectionVisitor import (
    AssetSelectionVisitor,
)
from dagster._core.definitions.asset_selection import (
    IS_ATTRIBUTE_VALUES,
    AssetSelection,
    AutomationTypeAssetSelection,
    ChangedInBranchAssetSelection,
    CodeLocationAssetSelection,
    ColumnAssetSelection,
    ColumnTagAssetSelection,
    GroupWildCardAssetSelection,
    IsAttributeAssetSelection,
    JobAssetSelection,
    KeyWildCardAssetSelection,
    PartitionsAssetSelection,
    ScheduleNameAssetSelection,
    SensorNameAssetSelection,
    StatusAssetSelection,
    TableNameAssetSelection,
)


class AntlrInputErrorListener(ErrorListener):
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise Exception(f"Syntax error at line {line}, column {column}: {msg}")


def parse_traversal_depth(optional_digits):
    """Return an integer from DIGITS text if present, otherwise None (meaning infinite depth)."""
    if optional_digits is None:
        return None
    return int(optional_digits.getText())


class AntlrAssetSelectionVisitor(AssetSelectionVisitor):
    def __init__(self, include_sources: bool):
        self.include_sources = include_sources

    def visitStart(self, ctx: AssetSelectionParser.StartContext):
        # pyrefly: ignore [bad-argument-type]
        return self.visit(ctx.expr())

    def visitTraversalAllowedExpression(
        self, ctx: AssetSelectionParser.TraversalAllowedExpressionContext
    ):
        # pyrefly: ignore [bad-argument-type]
        return self.visit(ctx.traversalAllowedExpr())

    # pyrefly: ignore [bad-override]
    def visitUpAndDownTraversalExpression(
        self, ctx: AssetSelectionParser.UpAndDownTraversalExpressionContext
    ):
        # pyrefly: ignore [bad-argument-type, bad-assignment]
        selection: AssetSelection = self.visit(ctx.traversalAllowedExpr())
        # upTraversal => optional DIGITS? PLUS
        # pyrefly: ignore [missing-attribute]
        up_digits = ctx.upTraversal().DIGITS()
        up_depth = parse_traversal_depth(up_digits)

        # downTraversal => PLUS DIGITS?
        # pyrefly: ignore [missing-attribute]
        down_digits = ctx.downTraversal().DIGITS()
        down_depth = parse_traversal_depth(down_digits)

        return selection.upstream(depth=up_depth) | selection.downstream(depth=down_depth)

    # pyrefly: ignore [bad-override]
    def visitUpTraversalExpression(self, ctx: AssetSelectionParser.UpTraversalExpressionContext):
        # pyrefly: ignore [bad-argument-type, bad-assignment]
        selection: AssetSelection = self.visit(ctx.traversalAllowedExpr())
        # pyrefly: ignore [missing-attribute]
        up_digits = ctx.upTraversal().DIGITS()
        up_depth = parse_traversal_depth(up_digits)
        return selection.upstream(depth=up_depth)

    # pyrefly: ignore [bad-override]
    def visitDownTraversalExpression(
        self, ctx: AssetSelectionParser.DownTraversalExpressionContext
    ):
        # pyrefly: ignore [bad-argument-type, bad-assignment]
        selection: AssetSelection = self.visit(ctx.traversalAllowedExpr())
        # pyrefly: ignore [missing-attribute]
        down_digits = ctx.downTraversal().DIGITS()
        down_depth = parse_traversal_depth(down_digits)
        return selection.downstream(depth=down_depth)

    # pyrefly: ignore [bad-override]
    def visitNotExpression(self, ctx: AssetSelectionParser.NotExpressionContext):
        # pyrefly: ignore [bad-argument-type, bad-assignment]
        selection: AssetSelection = self.visit(ctx.expr())
        return AssetSelection.all(include_sources=self.include_sources) - selection

    # pyrefly: ignore [bad-override]
    def visitAndExpression(self, ctx: AssetSelectionParser.AndExpressionContext):
        # pyrefly: ignore [bad-argument-type, bad-assignment]
        left: AssetSelection = self.visit(ctx.expr(0))
        # pyrefly: ignore [bad-argument-type, bad-assignment]
        right: AssetSelection = self.visit(ctx.expr(1))
        return left & right

    # pyrefly: ignore [bad-override]
    def visitOrExpression(self, ctx: AssetSelectionParser.OrExpressionContext):
        # pyrefly: ignore [bad-argument-type, bad-assignment]
        left: AssetSelection = self.visit(ctx.expr(0))
        # pyrefly: ignore [bad-argument-type, bad-assignment]
        right: AssetSelection = self.visit(ctx.expr(1))
        return left | right

    # pyrefly: ignore [bad-override]
    def visitAllExpression(self, ctx: AssetSelectionParser.AllExpressionContext):
        return AssetSelection.all(include_sources=self.include_sources)

    def visitAttributeExpression(self, ctx: AssetSelectionParser.AttributeExpressionContext):
        # pyrefly: ignore [bad-argument-type]
        return self.visit(ctx.attributeExpr())

    # pyrefly: ignore [bad-override]
    def visitFunctionCallExpression(self, ctx: AssetSelectionParser.FunctionCallExpressionContext):
        # pyrefly: ignore [bad-argument-type]
        function = self.visit(ctx.functionName())
        # pyrefly: ignore [bad-argument-type, bad-assignment]
        selection: AssetSelection = self.visit(ctx.expr())
        if function == "sinks":
            return selection.sinks()
        elif function == "roots":
            return selection.roots()

    def visitParenthesizedExpression(
        self, ctx: AssetSelectionParser.ParenthesizedExpressionContext
    ):
        # pyrefly: ignore [bad-argument-type]
        return self.visit(ctx.expr())

    # pyrefly: ignore [bad-override]
    def visitFunctionName(self, ctx: AssetSelectionParser.FunctionNameContext):
        if ctx.SINKS():
            return "sinks"
        elif ctx.ROOTS():
            return "roots"

    # pyrefly: ignore [bad-override]
    def visitKeyExpr(self, ctx: AssetSelectionParser.KeyExprContext):
        # pyrefly: ignore [bad-argument-type]
        value = self.visit(ctx.keyValue())
        # pyrefly: ignore [bad-argument-type]
        return KeyWildCardAssetSelection(selected_key_wildcard=value)

    # pyrefly: ignore [bad-override]
    def visitTagAttributeExpr(self, ctx: AssetSelectionParser.TagAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        key = self.visit(ctx.value(0))
        # pyrefly: ignore [bad-argument-type]
        value = self.visit(ctx.value(1)) if ctx.EQUAL() else None
        # pyrefly: ignore [bad-argument-type]
        return AssetSelection.tag(key, value or "", include_sources=self.include_sources)

    # pyrefly: ignore [bad-override]
    def visitOwnerAttributeExpr(self, ctx: AssetSelectionParser.OwnerAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        owner = self.visit(ctx.value())
        return AssetSelection.owner(owner)

    # pyrefly: ignore [bad-override]
    def visitGroupAttributeExpr(self, ctx: AssetSelectionParser.GroupAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        group = self.visit(ctx.value())
        if not group:
            return AssetSelection.groups(include_sources=self.include_sources)
        if "*" in group:
            return GroupWildCardAssetSelection(
                selected_group_wildcard=group, include_sources=self.include_sources
            )
        return AssetSelection.groups(group, include_sources=self.include_sources)

    # pyrefly: ignore [bad-override]
    def visitKindAttributeExpr(self, ctx: AssetSelectionParser.KindAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        kind = self.visit(ctx.value())
        return AssetSelection.kind(kind, include_sources=self.include_sources)

    # pyrefly: ignore [bad-override]
    def visitIsAttributeExpr(self, ctx: AssetSelectionParser.IsAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        value = self.visit(ctx.value())
        if value not in IS_ATTRIBUTE_VALUES:
            raise Exception(
                f"Invalid 'is:' attribute value {value!r}. "
                f"Supported values are: {sorted(IS_ATTRIBUTE_VALUES)}."
            )
        # pyrefly: ignore [bad-argument-type]
        return IsAttributeAssetSelection(attribute=value)

    # pyrefly: ignore [bad-override]
    def visitCodeLocationAttributeExpr(
        self, ctx: AssetSelectionParser.CodeLocationAttributeExprContext
    ):
        # pyrefly: ignore [bad-argument-type]
        code_location = self.visit(ctx.value())
        return CodeLocationAssetSelection(selected_code_location=code_location)

    def visitKeyValue(self, ctx: AssetSelectionParser.KeyValueContext):
        if ctx.QUOTED_STRING():
            # pyrefly: ignore [missing-attribute]
            return ctx.QUOTED_STRING().getText().strip('"')
        elif ctx.UNQUOTED_WILDCARD_STRING():
            # pyrefly: ignore [missing-attribute]
            return ctx.UNQUOTED_WILDCARD_STRING().getText()
        elif ctx.UNQUOTED_STRING():
            # pyrefly: ignore [missing-attribute]
            return ctx.UNQUOTED_STRING().getText()

    # pyrefly: ignore [bad-override]
    def visitValue(self, ctx: AssetSelectionParser.ValueContext):
        if ctx.QUOTED_STRING():
            # pyrefly: ignore [missing-attribute]
            return ctx.QUOTED_STRING().getText().strip('"')
        elif ctx.UNQUOTED_STRING():
            # pyrefly: ignore [missing-attribute]
            return ctx.UNQUOTED_STRING().getText()
        elif ctx.NULL_STRING():
            return None
        else:
            # Keyword tokens (SENSOR, SCHEDULE, JOB) used as values
            return ctx.getText()

    # pyrefly: ignore [bad-override]
    def visitStatusAttributeExpr(self, ctx: AssetSelectionParser.StatusAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        status = self.visit(ctx.value())
        return StatusAssetSelection(selected_status=status)

    # pyrefly: ignore [bad-override]
    def visitColumnAttributeExpr(self, ctx: AssetSelectionParser.ColumnAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        column = self.visit(ctx.value())
        return ColumnAssetSelection(selected_column=column)

    # pyrefly: ignore [bad-override]
    def visitTableNameAttributeExpr(self, ctx: AssetSelectionParser.TableNameAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        table_name = self.visit(ctx.value())
        return TableNameAssetSelection(selected_table_name=table_name)

    # pyrefly: ignore [bad-override]
    def visitColumnTagAttributeExpr(self, ctx: AssetSelectionParser.ColumnTagAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        key = self.visit(ctx.value(0))
        # pyrefly: ignore [bad-argument-type]
        value = self.visit(ctx.value(1)) if ctx.EQUAL() else None
        # pyrefly: ignore [bad-argument-type]
        return ColumnTagAssetSelection(key=key, value=value or "")

    # pyrefly: ignore [bad-override]
    def visitChangedInBranchAttributeExpr(
        self, ctx: AssetSelectionParser.ChangedInBranchAttributeExprContext
    ):
        # pyrefly: ignore [bad-argument-type]
        branch = self.visit(ctx.value())
        return ChangedInBranchAssetSelection(selected_changed_in_branch=branch)

    # pyrefly: ignore [bad-override]
    def visitPartitionsAttributeExpr(
        self, ctx: AssetSelectionParser.PartitionsAttributeExprContext
    ):
        # pyrefly: ignore [bad-argument-type]
        partitions = self.visit(ctx.value())
        return PartitionsAssetSelection(selected_partitions=partitions)

    # pyrefly: ignore [bad-override]
    def visitAutomationTypeAttributeExpr(
        self, ctx: AssetSelectionParser.AutomationTypeAttributeExprContext
    ):
        # pyrefly: ignore [bad-argument-type]
        automation_type = self.visit(ctx.value())
        return AutomationTypeAssetSelection(selected_automation_type=automation_type)

    # pyrefly: ignore [bad-override]
    def visitSensorAttributeExpr(self, ctx: AssetSelectionParser.SensorAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        sensor = self.visit(ctx.value())
        return SensorNameAssetSelection(selected_sensor=sensor)

    # pyrefly: ignore [bad-override]
    def visitScheduleAttributeExpr(self, ctx: AssetSelectionParser.ScheduleAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        schedule = self.visit(ctx.value())
        return ScheduleNameAssetSelection(selected_schedule=schedule)

    # pyrefly: ignore [bad-override]
    def visitJobAttributeExpr(self, ctx: AssetSelectionParser.JobAttributeExprContext):
        # pyrefly: ignore [bad-argument-type]
        job = self.visit(ctx.value())
        return JobAssetSelection(selected_job=job)


class AntlrAssetSelectionParser:
    def __init__(self, selection_str: str, include_sources: bool = False):
        lexer = AssetSelectionLexer(InputStream(selection_str))
        lexer.removeErrorListeners()  # Remove the default listener that just writes to the console
        lexer.addErrorListener(AntlrInputErrorListener())

        stream = CommonTokenStream(lexer)

        parser = AssetSelectionParser(stream)
        parser.removeErrorListeners()  # Remove the default listener that just writes to the console
        parser.addErrorListener(AntlrInputErrorListener())

        self._tree = parser.start()
        self._tree_str = self._tree.toStringTree(recog=parser)
        self._asset_selection = AntlrAssetSelectionVisitor(include_sources).visit(self._tree)

    @property
    def tree_str(self) -> str:
        return self._tree_str

    @property
    def asset_selection(self) -> AssetSelection:
        # pyrefly: ignore [bad-return]
        return self._asset_selection
