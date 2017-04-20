/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AllLab_Skeleton.Lab6;

/**
 *
 * @author bhavesh
 */
public class Person {
    final int id;
	final String firstName;
	final String lastName;
	final int birthYear;

	Person(final int id, String firstName, String lastName, int birthYear) {
		this.id = id;
		this.firstName = firstName;
		this.lastName = lastName;
		this.birthYear = birthYear;
	}
}
