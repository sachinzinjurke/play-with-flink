package com.bny.demo.modal;

import java.io.Serializable;
import java.util.Set;

public class Movie implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 521376886277977106L;
	private String name;
	private Set<String>genres;
	public Movie(String name, Set<String> genres) {
		super();
		this.name = name;
		this.genres = genres;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Set<String> getGenres() {
		return genres;
	}
	public void setGenres(Set<String> genres) {
		this.genres = genres;
	}
	@Override
	public String toString() {
		return "Movie [name=" + name + ", genres=" + genres + "]";
	}
	
	
}
