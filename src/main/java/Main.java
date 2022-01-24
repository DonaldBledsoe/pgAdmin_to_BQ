import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {

        Scanner myScanner = new Scanner(System.in);
        System.out.println("Enter an actor's name: ");
        String cmd_input = myScanner.nextLine();


        PipelineOptions options = PipelineOptionsFactory.create();
        options.setTempLocation("gs://york_temp_files/tmp");

        Pipeline p = Pipeline.create(options);

        TableReference table_spec =
                new TableReference()
                        .setProjectId("york-cdf-start")
                        .setDatasetId("final_donald_bledsoe_java")
                        .setTableId("PostGre_Table");

        TableSchema schema =
                new TableSchema()
                        .setFields(
                                Arrays.asList(
                                        new TableFieldSchema()                                  //schema table names HAVE to match what you pull in the query
                                                .setName("FirstName").setType("STRING"),
                                        new TableFieldSchema()
                                                .setName("LastName").setType("STRING"),
                                        new TableFieldSchema()
                                                .setName("Title").setType("STRING"),
                                        new TableFieldSchema()
                                                .setName("ID").setType("INT64"),
                                        new TableFieldSchema()
                                                .setName("Description").setType("STRING"),
                                        new TableFieldSchema()
                                                .setName("ReleaseYear").setType("INT64"),
                                        new TableFieldSchema()
                                                .setName("Language_id").setType("INT64"),
                                        new TableFieldSchema()
                                                .setName("CategoryName").setType("STRING"),
                                        new TableFieldSchema()
                                                .setName("Rental_duration").setType("INT64"),
                                        new TableFieldSchema()
                                                .setName("Rental_rate").setType("INT64"),
                                        new TableFieldSchema()
                                                .setName("Replacement_cost").setType("INT64")
                                )
                        );

        try {
            Class.forName("org.postgresql.Driver");

            // Retrieving data from database
            String queryString = "with CTE AS (SELECT a.first_name, a.last_name, UPPER(f.title) as title, f.film_id, f.description, \n" +
                    "                    f.release_year, f.language_id, f.rental_duration, f.rental_rate, f.replacement_cost, fc.film_id as category_film_id\n" +
                    "\n" +
                    "FROM actor a\n" +
                    "JOIN film_actor fa ON a.actor_id = fa.actor_id\n" +
                    "JOIN film f ON fa.film_id=f.film_id\n" +
                    "JOIN film_category fc ON f.film_id = fc.film_id\n" +
                    "\n" +
                    "WHERE a.first_name LIKE '" + cmd_input + "' \n" +
                    "OR a.last_name LIKE '" + cmd_input + "'),\n" +
                    "\n" +
                    "CTE_Category AS (SELECT fc.film_id, (CASE  \n" +
                    "            WHEN name LIKE 'D%'  \n" +
                    "                THEN UPPER(name)\n" +
                    "            WHEN name LIKE 'N%' \n" +
                    "                THEN LOWER (name)\n" +
                    "      \t\t\tELSE name\n" +
                    "             END) as category_name  FROM category c JOIN film_category fc ON c.category_id = fc.category_id\n" +
                    ")\n" +
                    "SELECT * FROM CTE JOIN CTE_Category ON CTE.category_film_id = CTE_Category.film_id\n" +
                    "\n" +
                    "LIMIT 250;";

            PCollection<TableRow> data = p.apply(JdbcIO.<TableRow>read()                //Make a PCollection of TableRows
                    // Data Source Configuration for PostgreSQL
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration         //Opening connection to AWS/PGAdmin

                            // Data Source Configuration for PostgreSQL
                            .create("org.postgresql.Driver", "jdbc:postgresql://ls-41d379b19b475ed294babb170cfa0f93b3011e47.cq2f1e9koedo.us-east-2.rds.amazonaws.com/donaldbledsoe77")

                            .withUsername("dbmasteruser").withPassword("Swnp3XQFtBd)b61NGn!uh{Lw=8#Vk~y<"))

                    .withQuery(queryString)
                    .withCoder(TableRowJsonCoder.of())                  //Needed because PCollection is a JSON
                    .withRowMapper(new JdbcIO.RowMapper<>() {
                                       private static final long serialVersionUID = 1L;

                                       public TableRow mapRow(ResultSet resultSet) throws Exception {
                                           TableRow outputTableRow = new TableRow();
                                           outputTableRow.set("FirstName",resultSet.getString("first_name"));
                                           outputTableRow.set("LastName",resultSet.getString("last_name"));
                                           outputTableRow.set("Title",resultSet.getString("title"));
                                           outputTableRow.set("ID",resultSet.getInt("film_id"));
                                           outputTableRow.set("Description",resultSet.getString("description"));
                                           outputTableRow.set("ReleaseYear",resultSet.getInt("release_year"));
                                           outputTableRow.set("Language_id",resultSet.getInt("language_id"));
                                           outputTableRow.set("CategoryName",resultSet.getString("category_name"));
                                           outputTableRow.set("Rental_duration",resultSet.getInt("rental_duration"));
                                           outputTableRow.set("Rental_rate",resultSet.getInt("rental_rate"));
                                           outputTableRow.set("Replacement_cost",resultSet.getInt("replacement_cost"));

                                           return outputTableRow;

                                       }
                                   }
                    ));


            //WriteToBigQuery
            data.apply(
                    "Write to BigQuery1",
                    BigQueryIO.writeTableRows()
                            .to(table_spec)
                            .withSchema(schema)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

            p.run().waitUntilFinish();

        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
