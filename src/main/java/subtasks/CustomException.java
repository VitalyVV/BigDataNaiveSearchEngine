package subtasks;

public class CustomException extends RuntimeException {
  private String temp;

  public CustomException(String desc, Throwable t, String fname) {
    super(desc, t);
    temp = fname;
  }

  @Override
  public String toString() {
    StringBuffer theString = new StringBuffer(super.toString());

    return String.format("FNAME = [%s]\n%s", temp, theString.toString());
  }
}
