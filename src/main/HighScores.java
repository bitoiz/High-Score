package main;
/**
 *
 * @author MAZ
 */
import java.util.InputMismatchException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
//
import highscores.DBService;
//
public final class HighScores {

  /**
   * @param args the command line arguments
   */
  public static void main (final String[] args) {
    
    final DBService dbService;
    try {
      dbService = new DBService();
    } catch (final SQLException | NoSuchAlgorithmException ex) {
      System.err.println("Problema interno de la aplicación");
      System.err.println("Informe al administrador");      
      return;
    }

    final Scanner scanner = new Scanner(System.in);
    int opcion;
    do {

      System.out.println("Opciones:");
      System.out.println("  1 - Añadir nueva puntuación");
      System.out.println("  2 - Consultar puntuaciones por jugador");
      System.out.println("  3 - Consultar puntuaciones por juego");
      System.out.println("  4 - Añadir nuevo jugador");
      System.out.println("  5 - Añadir nuevo juego");
      System.out.println("  6 - Listar jugadores");
      System.out.println("  7 - Listar juegos");
      System.out.println("  0 - Salir");
      System.out.print("Introduce opcion: ");
      
      try {
        opcion = scanner.nextInt();
        scanner.nextLine();

        switch (opcion) {
          case 1: {
            final String[] data = getNewHighScoreInputData(scanner);
            try {
              final boolean result = dbService.newHighScore(data);
              if (result) {
                System.out.println("Nueva puntuación almacenada");
              } else {
                System.out.println("Revise que jugador y juego estén registrados");
              }             
            } catch (final SQLException ex) {
              System.err.println("Puntuación no incluida (problema en proceso con BBDD)");
              System.err.println("Pregunte al administrador");                 
            }
            break;
          }

          case 2: {
            final String player = getPlayerInputData(scanner);
            final Map<String, Long> scores = dbService.highScoresByPlayer(player);
            if (!scores.isEmpty()) {
              System.out.println("Puntuaciones del jugador " + player + ": ");
              for (final String name: scores.keySet())
                System.out.println("\t" + name + ": " + scores.get(name));
            } else {
              System.out.println("No hay puntuaciones para el jugador "  + player);
            }
            break;
          }
          
          case 3: {
            final String game = getGameInputData(scanner);
            final Map<String, Long> scores = dbService.highScoresByGame(game);
            if (!scores.isEmpty()) {
              System.out.println("Puntuaciones registradas para el juego " + game + ": ");
              for (final String name: scores.keySet())
                System.out.println("\t" + name + ": " + scores.get(name));
            } else {
              System.out.println("No hay puntuaciones para el juego "  + game);
            }
            break;
          }
          
          case 4: {
            final String player = getPlayerInputData(scanner);
            try {
              final boolean result = dbService.newPlayer(player);
              if (result) {
                System.out.println("Nuevo jugador registrado");
              } else {
                System.out.println("Jugador " + player + " ya está registrado");
              }            
            } catch (final SQLException ex) {
              System.out.println("Jugador no registrado (problema en almacenamiento en BBDD)");
              System.err.println("Pregunte al administrador");              
            }
            break;
          }    
          
          case 5: {
            final String game = getGameInputData(scanner);
            try {
              final boolean result = dbService.newGame(game);
              if (result) {
                System.out.println("Nuevo juego registrado");
              } else {
                System.out.println("Juego " + game + " ya está registrado");
              }            
            } catch (final SQLException ex) {
              System.out.println("Juego no registrado (problema en almacenamiento en BBDD)");
              System.err.println("Pregunte al administrador");
            }
            break;
          }
          
          case 6: {
            final List<String> players = dbService.getPlayers();
            if (!players.isEmpty()) {
              System.out.println("Listado de jugadores: ");
              for (final String name: players)
                System.out.println("\t" + name);
            } else {
              System.out.println("No hay jugadores");
            }
            break;
          }
          
          case 7: {
            final List<String> games = dbService.getGames();
            if (!games.isEmpty()) {
              System.out.println("Listado de juegos: ");
              for (final String name: games)
                System.out.println("\t" + name);
            } else {
              System.out.println("No hay juegos");
            }
            break;
          }
          
          default: {
          }
        }

      } catch (final InputMismatchException ex) {
        scanner.nextLine();
        opcion = Integer.MAX_VALUE;
      }

    } while (opcion != 0);
    
    // Se cierra la conexión con la base de datos.
    dbService.close();

  }
  
  static private String[] getNewHighScoreInputData (final Scanner scanner) {
    final String[] data = new String[3];
    data[0] = getPlayerInputData(scanner);
    data[1] = getGameInputData(scanner);    
    data[2] = getHighScoreInputData(scanner);        
    return data;
  }
  
  static private String getPlayerInputData (final Scanner scanner) {
    System.out.print("Introduzca nombre de jugador: ");
    final String data = scanner.nextLine().trim();
    return data;
  }
  
  static private String getGameInputData (final Scanner scanner) {
    System.out.print("Introduzca nombre de juego: ");
    final String data = scanner.nextLine().trim();
    return data;
  }
  
  static private String getHighScoreInputData (final Scanner scanner) {
    System.out.print("Introduzca puntuación: ");
    final String data = Long.toString(scanner.nextLong());
    scanner.nextLine();
    return data;
  }    

}
