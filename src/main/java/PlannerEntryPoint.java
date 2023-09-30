import calcite.rules.ArrowTableScanFilterProjectRule;
import calcite.rules.ArrowTableScanProjectionRule;
import calcite.rules.ArrowTableScanRule;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import util.arrow.ArrowSchemaBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;

public class PlannerEntryPoint {


    private final static HepPlanner aethraHepPlanner;

    static {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        hepProgramBuilder.addRuleInstance(AggregateReduceFunctionsRule.Config.DEFAULT.toRule());
        hepProgramBuilder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule());
        hepProgramBuilder.addRuleInstance(ProjectJoinTransposeRule.Config.DEFAULT.toRule());
        hepProgramBuilder.addRuleInstance(ProjectRemoveRule.Config.DEFAULT.toRule());
        final ArrowTableScanProjectionRule PROJECT_SCAN = ArrowTableScanProjectionRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(PROJECT_SCAN);
        final ArrowTableScanRule ARROW_SCAN = ArrowTableScanRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(ARROW_SCAN);
        final ArrowTableScanFilterProjectRule ARROW_SCAN_FILTER_PROJECT = ArrowTableScanFilterProjectRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(ARROW_SCAN_FILTER_PROJECT);

        aethraHepPlanner = new HepPlanner(hepProgramBuilder.build());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IllegalArgumentException("You need to supply the database path and the query file as arguments.");

        String databasePath = args[0];
        String queryPath = args[1];

        // Read the schema from disk
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        CalciteSchema databaseSchema = ArrowSchemaBuilder.fromDirectory(databasePath, typeFactory);

        // Initialise the planner
        SqlParser.Config sqlParserConfig = SqlParser.config().withCaseSensitive(false);
        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(sqlParserConfig)
                .defaultSchema(databaseSchema.plus())
                .build();
        PlannerImpl queryPlanner = new PlannerImpl(frameworkConfig);

        // First, parse the query
        BufferedReader queryReader = new BufferedReader(new FileReader(queryPath));
        SqlNode parsedSqlQuery = queryPlanner.parse(queryReader);

        // Next, validate the query
        SqlNode validatedSqlQuery = queryPlanner.validate(parsedSqlQuery);
        RelNode queryRoot = queryPlanner.rel(validatedSqlQuery).project();

        // Finally, plan/optimise the query
        aethraHepPlanner.setRoot(queryRoot);
        RelNode optimisedQuery = aethraHepPlanner.findBestExp();

        // Output the optimised query
        var relWriter = new RelWriterImpl(new PrintWriter(System.out, true), SqlExplainLevel.NON_COST_ATTRIBUTES, false);
        optimisedQuery.explain(relWriter);

        // Close the planner
        queryPlanner.close();
    }

}