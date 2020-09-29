package com.spotify.folsom.client;

public class Flags {

  private final int flags;

  public static final Flags DEFAULT = new Flags(0);

  public Flags(int flags) {
    this.flags = flags;
  }

  public int asInt() {
    return flags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Flags flags1 = (Flags) o;
    return flags == flags1.flags;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(flags);
  }
}
