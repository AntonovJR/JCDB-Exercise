import java.sql.*;
import java.util.*;

public class Main {
    private static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/";
    private static final String DB_NAME = "minions_db";
    private static Connection connection;
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws SQLException {
        connection = getConnection();
        System.out.println("Enter exercise number: ");
        int exNum = Integer.parseInt(scanner.nextLine());
        switch (exNum) {
            case 2 -> secondTask();
            case 3 -> thirdTask();
            case 4 -> fourthTask();
            case 5 -> fifthTask();
            case 6 -> sixthTask();
            case 7 -> seventhTask();
            case 8 -> eightTask();
            case 9 -> ninthTask();
            default -> System.out.println("Invalid exercise number.");
        }


    }

    private static void ninthTask() throws SQLException {
        System.out.println("Enter ID: ");
        int id = Integer.parseInt(scanner.nextLine());

        CallableStatement callableStatement = connection.prepareCall("{ call usp_get_older(?) }");
        callableStatement.setInt(1, id);
        PreparedStatement preparedStatement = connection.prepareStatement(" Select `name`, `age` FROM minions WHERE `id` = ?;");
        preparedStatement.setInt(1, id);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            System.out.println(resultSet.getString("name") + " " + resultSet.getInt("age"));
        }


    }

    private static void eightTask() throws SQLException {
        System.out.println("Enter IDs: ");
        int[] tokens = Arrays.stream(scanner.nextLine().split("\\s+")).mapToInt(Integer::parseInt).toArray();
        PreparedStatement preparedStatement = connection.prepareStatement(" UPDATE minions SET `name` = LOWER(`name`), `age` = `age`+1 WHERE `id` = ?;");
        for (int token : tokens) {
            preparedStatement.setInt(1, token);
            preparedStatement.executeUpdate();

        }

        preparedStatement = connection.prepareStatement("SELECT `name`, `age` FROM minions;");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("name") + " " + resultSet.getInt("age"));
        }

    }


    private static void seventhTask() throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT `name` FROM minions;");
        ResultSet resultSet = preparedStatement.executeQuery();

        List<String> minions = new ArrayList<>();
        while (resultSet.next()) {
            minions.add(resultSet.getString("name"));
        }
        for (int i = 0; i < minions.size() / 2; i++) {
            System.out.println(minions.get(i));
            System.out.println(minions.get(minions.size() - i - 1));

        }
    }

    private static void sixthTask() throws SQLException {
        System.out.println("Enter villain ID: ");
        int villainID = Integer.parseInt(scanner.nextLine());
        String villainName = findVillainNameById(villainID);
        if (villainName == null) {
            System.out.println("No such villain was found");
            System.exit(0);
        } else {
            System.out.printf("%s was deleted%n", villainName);
        }

        PreparedStatement preparedStatement = connection.prepareStatement("delete  from minions_villains\n" +
                "Where villain_id = ?;");
        preparedStatement.setInt(1, villainID);
        int countMinions = preparedStatement.executeUpdate();
        System.out.printf("%d minions released%n", countMinions);

        preparedStatement = connection.prepareStatement("DELETE FROM villains\n" +
                "WHERE id = ?;");
        preparedStatement.setInt(1, villainID);
        preparedStatement.execute();

    }

    private static void fifthTask() throws SQLException {
        System.out.println("Enter country name: ");
        String countryName = scanner.nextLine();
        PreparedStatement preparedStatement = connection.prepareStatement("UPDATE towns set `name` = UPPER(`name`)\n" +
                "WHERE `country` = ?;");
        preparedStatement.setString(1, countryName);
        int affectedRows = preparedStatement.executeUpdate();
        if (affectedRows == 0) {
            System.out.println("No town names were affected.");
        } else {
            System.out.printf("%d town names were affected.%n", affectedRows);
            preparedStatement = connection.prepareStatement("SELECT `name` from towns\n" +
                    "where country = ?;");
            preparedStatement.setString(1, countryName);
            ResultSet resultSet = preparedStatement.executeQuery();
            List<String> towns = new ArrayList<>();
            while (resultSet.next()) {
                towns.add(resultSet.getString("name"));
            }
            System.out.println(towns);
        }

    }

    private static void fourthTask() throws SQLException {
        String[] minionTokens = scanner.nextLine().split("\\s+");
        String[] villainTokens = scanner.nextLine().split("\\s+");

        String minionName = minionTokens[1];
        int minionAge = Integer.parseInt(minionTokens[2]);
        String minionTown = minionTokens[3];

        String villainName = villainTokens[1];
        String villainEvilnessFactor = "evil";
        if (villainTokens.length == 3) {
            villainEvilnessFactor = villainTokens[2];
        }
        addTown(minionTown);
        addMinion(minionName, minionAge);
        addVillain(villainName, villainEvilnessFactor);
        addMinionToVillain(minionName, villainName);

    }

    private static void addMinionToVillain(String minionName, String villainName) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("DELETE from minions_villains\n" +
                "WHERE minion_id = (SELECT `id` FROM minions WHERE name = ?);");
        preparedStatement.setString(1, minionName);
        preparedStatement.execute();

        preparedStatement = connection.prepareStatement("""
                SELECT `minion_id`, `villain_id` FROM minions_villains
                JOIN minions m on m.id = minions_villains.minion_id
                JOIN villains v on v.id = minions_villains.villain_id
                Where m.name = ? AND v.name = ?;""");
        preparedStatement.setString(1, minionName);
        preparedStatement.setString(2, villainName);
        ResultSet resultSet = preparedStatement.executeQuery();

        if (!resultSet.next()) {
            preparedStatement = connection.prepareStatement("INSERT INTO minions_villains(minion_id, villain_id) " +
                    "VALUE ((SELECT `id` FROM `minions` WHERE `name` = ?),(SELECT `id` FROM `villains` WHERE `name` = ?))");
            preparedStatement.setString(1, minionName);
            preparedStatement.setString(2, villainName);
            preparedStatement.execute();
            System.out.printf("Successfully added %s to be minion of %s%n", minionName, villainName);
        }
    }

    private static void addVillain(String villainName, String villainEvilnessFactor) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT `name` from villains\n" +
                "where name = ?;");
        preparedStatement.setString(1, villainName);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (!resultSet.next()) {
            preparedStatement = connection.prepareStatement("""
                    INSERT INTO `villains` (name, evilness_factor)
                    Values
                    (?,?);""");
            preparedStatement.setString(1, villainName);
            preparedStatement.setString(2, villainEvilnessFactor);
            preparedStatement.execute();
            System.out.printf("Villain %s was added to the database.%n", villainName);
        }
    }

    private static void addMinion(String minionName, int minionAge) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT `name`, `age` from minions\n" +
                "where name = ? AND `age` = ?;");
        preparedStatement.setString(1, minionName);
        preparedStatement.setInt(2, minionAge);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (!resultSet.next()) {
            preparedStatement = connection.prepareStatement("""
                    INSERT INTO `minions` (name, age)
                    Values
                    (?,?);""");
            preparedStatement.setString(1, minionName);
            preparedStatement.setInt(2, minionAge);
            preparedStatement.execute();
        }
    }

    private static void addTown(String minionTown) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT `id` from towns\n" +
                "where towns.name = ?;");
        preparedStatement.setString(1, minionTown);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (!resultSet.next()) {
            preparedStatement = connection.prepareStatement("""
                    INSERT INTO `towns` (name)
                    Values
                    (?);""");
            preparedStatement.setString(1, minionTown);
            preparedStatement.execute();
            System.out.printf("Town %s was added to the database.%n", minionTown);
        }
    }


    private static void thirdTask() throws SQLException {
        System.out.println("Enter villain id: ");
        int villainId = Integer.parseInt(scanner.nextLine());
        String villainName = findVillainNameById(villainId);
        if (villainName == null) {
            System.out.printf("No villain with ID %d exists in the database.", villainId);
            System.exit(0);
        } else {
            System.out.println("Villain: " + villainName);
        }
        getMinions(villainId);
    }

    private static void getMinions(int villainId) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT m.`name`,m.`age` FROM `minions` AS m
                JOIN minions_villains mv ON m.id = mv.minion_id
                JOIN villains v ON v.id = mv.villain_id
                WHERE mv.villain_id = ?;""");
        preparedStatement.setInt(1, villainId);
        ResultSet resultSet = preparedStatement.executeQuery();
        int count = 1;
        while (resultSet.next()) {
            System.out.printf("%d. %s %d%n", count++, resultSet.getString("name"), resultSet.getInt("age"));
        }
    }

    private static String findVillainNameById(int villainId) throws SQLException {
        PreparedStatement preparedStatement =
                connection.prepareStatement("SELECT `name`FROM villains WHERE id = ?");
        preparedStatement.setInt(1, villainId);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getString("name");
        }
        return null;

    }

    private static void secondTask() throws SQLException {
        PreparedStatement preparedStatement =
                connection.prepareStatement("""
                        SELECT  v.name, COUNT(DISTINCT mv.minion_id) as m_count FROM villains v
                        JOIN minions_villains mv on v.id = mv.villain_id
                        GROUP BY v.name
                        HAVING m_count>15;""");

        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            System.out.printf("%s %d %n", resultSet.getString("name"), resultSet.getInt("m_count"));
        }
    }

    public static Connection getConnection() throws SQLException {
        System.out.println("Enter user: ");
        String user = scanner.nextLine();

        System.out.println("Enter password: ");
        String password = scanner.nextLine();

        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        return DriverManager.getConnection(CONNECTION_STRING + DB_NAME, properties);
    }
}
