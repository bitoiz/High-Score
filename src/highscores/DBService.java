package highscores;
/**
 *
 * @author MAZ
 */
import java.io.File;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
//
public final class DBService {

  static private final Logger LOGGER = Logger.getLogger(DBService.class.getName());

  static private final String DB_USER = "labops";
  static private final String DB_PASSWORD = "trustnoone";

  private final MessageDigest md;
  private final Connection connection;

  public DBService () throws SQLException, NoSuchAlgorithmException {

    // Servicio de resumen digital; se emplea para obtener una clave única
    // a partir del nombre del juegor o del juego.
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (final NoSuchAlgorithmException ex) {
      LOGGER.info("algoritmo de resumen no provisto en la plataforma");
      LOGGER.severe(ex.getMessage());
      throw new NoSuchAlgorithmException();
    }

    // Conexión con la BBDD; se mantiene abierta durante toda la ejecución (por
    // esa razón no se emplea try-con-recursos).
    try {

      final String db_URL = "jdbc:h2:file:" + System.getProperty("user.dir") +
                                                     File.separator + "data" +
                                                     File.separator + "database" +
                                                     File.separator + "highscoresDB";
      connection = DriverManager.getConnection(db_URL, DB_USER, DB_PASSWORD);

    } catch (final SQLTimeoutException ex) {
      LOGGER.info("timeout al intentar establecer la conexion");
      LOGGER.severe(ex.getMessage());
      throw new SQLException();
    } catch (final SQLException ex) {
      LOGGER.info("error al abrir la conexion");
      LOGGER.severe(ex.getMessage());
      throw new SQLException();
    }

  }

  public void close () {
    try {
      connection.close();
    } catch (final SQLException ex) {
      LOGGER.info("error al cerrar la conexion");
      LOGGER.severe(ex.getMessage());
    }
  }

  private String getID (final String data) {
    // Generación de una clave única a partir del string recibido.
    // La clave generada es una secuencia de 16 octetos. Para
    // poder emplear la clave generada en los strings de las sentencias,
    // hay que convertir esa secuencia de octetos en un string que codifique
    // la misma información. Con ese fin se emplea la codificación Base64.
    final byte[] _Id = md.digest(data.getBytes(Charset.forName("UTF-8")));
    final String  Id = Base64.getEncoder().encodeToString(_Id);
    return Id;
  }

  public boolean newHighScore (final String[] data) throws SQLException {

    final String player = data[0];
    final String game = data[1];
    final int score = Integer.parseInt(data[2]);

    // Jugador y juego deben estar registrados
    if (!registeredPlayer(player) || !registeredGame(game))
      return false;

    // Se ha verificado que jugador y juego están registrados
    try (final Statement statement = connection.createStatement()) {

      // Por comodidad, para evitar tener que aplicar cast, los valores
      // de las claves son strings que representan 16 octetos en Base64.
      final String playerId = getID(player);
      final String   gameId = getID(game);

      final String insertScore =
              "INSERT INTO scores (playerID, gameID, score) VALUES ('"
              + playerId + "','" + gameId + "','" + score + "')";

      return statement.executeUpdate(insertScore) == 1;

    } catch (final SQLException ex) {
      LOGGER.info("problema al realizar la inserción de nueva puntuación con");
      LOGGER.log(Level.INFO, "jugador {0}, juego {1} y puntuación {2}", data);
      LOGGER.severe(ex.getMessage());
    }

    return false;

  }

  public Map<String, Long> highScoresByPlayer (final String player) {
    final Map<String, Long> scores = new LinkedHashMap<>();
    try (final Statement statement = connection.createStatement()) {
      final String scoresQueryPart1 =
              "SELECT Games.name, GS.score FROM Games INNER JOIN ";
      final String playerId =
              "(SELECT playerID FROM Players WHERE name = '" + player + "')";
      final String scoresQueryPart2 =
              "(SELECT gameID, score FROM Scores WHERE playerID = " + playerId + ") GS ";
      final String scoresQueryPart3 =
              "ON Games.gameID = GS.gameID ORDER BY GS.score";
      final String scoresQuery = scoresQueryPart1 + scoresQueryPart2 + scoresQueryPart3;
      final ResultSet rs = statement.executeQuery(scoresQuery);
      while (rs.next()) {
        scores.put(rs.getString("Games.name"), rs.getLong("GS.score"));
      }
    } catch (final SQLException ex) {
      LOGGER.log(Level.INFO,
              "problema al consultar puntuaciones de jugador {0}", player);
      LOGGER.severe(ex.getMessage());
    }
    return scores;
  }

  public Map<String, Long> highScoresByGame (final String game) {
    final Map<String, Long> scores = new LinkedHashMap<>();
    try (final Statement statement = connection.createStatement()) {
      final String scoresQueryPart1 =
              "SELECT Players.name, PS.score FROM Players INNER JOIN ";
      final String gameId =
              "(SELECT gameID FROM Games WHERE name = '" + game + "')";
      final String scoresQueryPart2 =
              "(SELECT playerID, score FROM Scores WHERE gameID = " + gameId + ") PS ";
      final String scoresQueryPart3 =
              "ON Players.playerID = PS.playerID ORDER BY PS.score";
      final String scoresQuery = scoresQueryPart1 + scoresQueryPart2 + scoresQueryPart3;
      final ResultSet rs = statement.executeQuery(scoresQuery);
      while (rs.next()) {
        scores.put(rs.getString("Players.name"), rs.getLong("PS.score"));
      }
    } catch (final SQLException ex) {
      LOGGER.log(Level.INFO,
              "problema al consultar puntuaciones de juego {0}", game);
      LOGGER.severe(ex.getMessage());
    }
    return scores;
  }

  public List<String> getPlayers () {
    final List<String> players = new ArrayList<>();
    try (final Statement statement = connection.createStatement()) {
      final String playersQuery = "SELECT name FROM players";
      final ResultSet rs = statement.executeQuery(playersQuery);
      while (rs.next()) {
        players.add(rs.getString("name"));
      }
    } catch (final SQLException ex) {
      LOGGER.info("problema al consultar listado de jugadores");
      LOGGER.severe(ex.getMessage());
    }
    return players;
  }

  public List<String> getGames () {
    final List<String> games = new ArrayList<>();
    try (final Statement statement = connection.createStatement()) {
      final String gamesQuery = "SELECT name FROM games";
      final ResultSet rs = statement.executeQuery(gamesQuery);
      while (rs.next()) {
        games.add(rs.getString("name"));
      }
    } catch (final SQLException ex) {
      LOGGER.info("problema al consultar listado de juegos");
      LOGGER.severe(ex.getMessage());
    }
    return games;
  }

  public boolean newPlayer (final String player) throws SQLException {

    if (registeredPlayer(player)) // Jugador ya está registrado
      return false;

    // Jugador no registrado
    try (final Statement statement = connection.createStatement()) {

      final String playerId = getID(player);
      System.out.println(playerId);
      final String playerInsert =
              "INSERT INTO players (playerID, name) VALUES ('" + playerId + "','" + player + "')";
      statement.executeUpdate(playerInsert);

      return true;

    } catch (final SQLException ex) {
      LOGGER.log(Level.INFO,
              "problema al intentar inserción en tabla PLAYERS con jugador {0}", player);
      LOGGER.severe(ex.getMessage());
      throw new SQLException();
    }

  }

  public boolean newGame (final String game) throws SQLException {

    if (registeredGame(game)) // Juego ya está registrado
      return false;

    // Juego no registrado
    try (final Statement statement = connection.createStatement()) {

      final String gameId = getID(game);
      System.out.println(gameId);
      final String gameInsert =
              "INSERT INTO games (gameID, name) VALUES ('" + gameId + "','" + game + "')";
      statement.executeUpdate(gameInsert);

      return true;

    } catch (final SQLException ex) {
      LOGGER.log(Level.INFO,
              "problema al intentar inserción en tabla GAMES con juego {0}", game);
      LOGGER.severe(ex.getMessage());
      throw new SQLException();
    }

  }

  private boolean registeredPlayer (final String player) throws SQLException {

    try (final Statement statement = connection.createStatement()) {

      final String playerQuery = "SELECT * FROM players WHERE name = '" + player + "'";
      final ResultSet rs = statement.executeQuery(playerQuery);

      return rs.next(); // Método next() devuelve true si y solo si rs no está vacío.

    } catch  (final SQLException ex) {
      LOGGER.log(Level.INFO,
              "problema al realizar consulta en tabla PLAYERS con jugador {0}", player);
      LOGGER.severe(ex.getMessage());
      throw new SQLException();
    }

  }

  private boolean registeredGame (final String game) throws SQLException {

    try (final Statement statement = connection.createStatement()) {

      final String playerQuery = "SELECT * FROM games WHERE name = '" + game + "'";
      final ResultSet rs = statement.executeQuery(playerQuery);

      return rs.next(); // Método next() devuelve true si y solo si rs no está vacío.

    } catch  (final SQLException ex) {
      LOGGER.log(Level.INFO,
              "problema al realizar consulta en tabla GAMES con juego {0}", game);
      LOGGER.severe(ex.getMessage());
      throw new SQLException();
    }

  }

}
