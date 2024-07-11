/* Generated By:JavaCC: Do not edit this line. DatalogParserConstants.java */
package com.getl.datalog.cc;


/** 
 * Token literal values and constants.
 * Generated by org.javacc.parser.OtherFilesGen#start()
 */
public interface DatalogParserConstants {

  /** End of File. */
  int EOF = 0;
  /** RegularExpression Id. */
  int WHITESPACE = 1;
  /** RegularExpression Id. */
  int VAR_NAME = 2;
  /** RegularExpression Id. */
  int GREMLIN = 3;
  /** RegularExpression Id. */
  int PRED_CONST_NAME = 4;
  /** RegularExpression Id. */
  int GREMLIN_BLOCK = 5;
  /** RegularExpression Id. */
  int PERIOD = 6;
  /** RegularExpression Id. */
  int COMMA = 7;

  /** Lexical state. */
  int DEFAULT = 0;

  /** Literal token values. */
  String[] tokenImage = {
    "<EOF>",
    "<WHITESPACE>",
    "<VAR_NAME>",
    "\"gremlin\"",
    "<PRED_CONST_NAME>",
    "<GREMLIN_BLOCK>",
    "\".\"",
    "\",\"",
    "\":-\"",
    "\"(\"",
    "\")\"",
  };

}
