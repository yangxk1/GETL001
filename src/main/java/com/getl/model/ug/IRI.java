package com.getl.model.ug;

public class IRI implements CharSequence {

    private final String nameSpace;
    private final String localName;

    public IRI(String nameSpace, String localName) {
        this.nameSpace = nameSpace;
        this.localName = localName;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public String getLocalName() {
        return localName;
    }

    public String toString() {
        return nameSpace + localName;
    }

    @Override
    public int length() {
        return nameSpace.length() + localName.length();
    }

    @Override
    public char charAt(int index) {
        return (nameSpace + localName).charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return (nameSpace + localName).subSequence(start, end);
    }

}
