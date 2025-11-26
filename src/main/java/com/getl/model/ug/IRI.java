package com.getl.model.ug;

public class IRI implements CharSequence {

    private final String nameSpaceId;
    private final Object localName;

    public IRI(String nameSpaceId, Object localName) {
        this.nameSpaceId = nameSpaceId;
        this.localName = localName;
    }

    public String getNameSpaceId() {
        return nameSpaceId;
    }

    public String getLocalName() {
        return localName.toString();
    }
    public Object getLocalID() {return this.localName;}

    public String toString() {
        //TODO
        return nameSpaceId + localName;
    }

    @Override
    public int length() {
        return nameSpaceId.length() + localName.toString().length();
    }

    @Override
    public char charAt(int index) {
        return (nameSpaceId + localName).charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return (nameSpaceId + localName).subSequence(start, end);
    }

}
