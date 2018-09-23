package ru.ifmo.rain.telnoj.student;

import info.kgeorgiy.java.advanced.student.Group;
import info.kgeorgiy.java.advanced.student.Student;
import info.kgeorgiy.java.advanced.student.StudentGroupQuery;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StudentDB implements StudentGroupQuery {

    @Override
    public List<String> getFirstNames(List<Student> students) {
        return getValuesList(students, Student::getFirstName);
    }

    @Override
    public List<String> getLastNames(List<Student> students) {
        return getValuesList(students, Student::getLastName);
    }

    @Override
    public List<String> getGroups(List<Student> students) {
        return getValuesList(students, Student::getGroup);
    }

    @Override
    public List<String> getFullNames(List<Student> students) {
        return getValuesList(students, s -> s.getFirstName() + " " + s.getLastName());
    }

    @Override
    public Set<String> getDistinctFirstNames(List<Student> students) {
        return mapStringStream(students, Student::getFirstName).collect(Collectors.toCollection(TreeSet::new));
    }

    @Override
    public String getMinStudentFirstName(List<Student> students) {
        return getStream(students).min(idComparator).map(Student::getFirstName).orElse("");
    }

    @Override
    public List<Student> sortStudentsById(Collection<Student> students) {
        return getSortedStudentList(getStream(students), idComparator);
    }

    @Override
    public List<Student> sortStudentsByName(Collection<Student> students) {
        return getSortedStudentList(getStream(students), nameComparator);
    }

    @Override
    public List<Student> findStudentsByFirstName(Collection<Student> students, String name) {
        return getSortedFilteredStudentListByName(students, Student::getFirstName, name);
    }

    @Override
    public List<Student> findStudentsByLastName(Collection<Student> students, String name) {
        return getSortedFilteredStudentListByName(students, Student::getLastName, name);
    }

    @Override
    public List<Student> findStudentsByGroup(Collection<Student> students, String group) {
        return getSortedStudentList(getStreamByGroup(students, group), nameComparator);
    }

    @Override
    public Map<String, String> findStudentNamesByGroup(Collection<Student> students, String group) {
        return getStreamByGroup(students, group).collect(Collectors.toMap(
                Student::getLastName, Student::getFirstName, BinaryOperator.minBy(String::compareTo)
        ));
    }

    @Override
    public List<Group> getGroupsByName(Collection<Student> students) {
        return getGroupsSortedList(students, nameComparator);
    }

    @Override
    public List<Group> getGroupsById(Collection<Student> students) {
        return getGroupsSortedList(students, idComparator);
    }

    @Override
    public String getLargestGroup(Collection<Student> students) {
        return getCollectedLargestGroupName(students, Collectors.counting());
    }

    @Override
    public String getLargestGroupFirstName(Collection<Student> students) {
        return getCollectedLargestGroupName(students, Collectors.collectingAndThen(Collectors.mapping(Student::getFirstName,
                Collectors.toSet()), s -> (long) s.size()));
    }

    private static final Comparator<Student> nameComparator = Comparator.comparing(Student::getLastName)
            .thenComparing(Student::getFirstName).thenComparingInt(Student::getId);

    private static final Comparator<Student> idComparator = Comparator.comparingInt(Student::getId);

    private <T> Stream<T> getStream(Collection<T> collection) {
        return collection.stream();
    }

    private List<String> getValuesList(List<Student> students, Function<Student, String> function) {
        return collectToList(mapStringStream(students, function));
    }

    private List<Student> getSortedStudentList(Stream<Student> students, Comparator<Student> comparator) {
        return collectToList(students.sorted(comparator));
    }

    private Stream<String> mapStringStream(List<Student> students, Function<Student, String> function) {
        return getStream(students).map(function);
    }

    private <T> List<T> collectToList(Stream<T> stream) {
        return stream.collect(Collectors.toList());
    }

    private Stream<Student> getFilteredStream(Collection<Student> students, Function<Student, String> function, String key) {
        return getStream(students).filter(s -> function.apply(s).equals(key));
    }

    private List<Student> getSortedFilteredStudentListByName(Collection<Student> students, Function<Student, String> getName, String name) {
        return getSortedStudentList(getFilteredStream(students, getName, name), nameComparator);
    }

    private Stream<Student> getStreamByGroup(Collection<Student> students, String group) {
        return getFilteredStream(students, Student::getGroup, group);
    }

    private <T> Stream<Map.Entry<String, T>> getCollectedInGroupsMapStream(Collection<Student> students, Collector<? super Student, ?, T> collector) {
        return getStream(getStream(students).collect(Collectors.groupingBy(Student::getGroup, TreeMap::new, collector)).entrySet());
    }

    private List<Group> getGroupsSortedList(Collection<Student> students, Comparator<Student> comparator) {
        return collectToList(getCollectedInGroupsMapStream(students, Collectors.mapping(Function.identity(),
                Collectors.toCollection(() -> new TreeSet<>(comparator))))
                .map(g -> new Group(g.getKey(), new LinkedList<>(g.getValue()))));
    }

    private String getCollectedLargestGroupName(Collection<Student> students, Collector<? super Student, ?, Long> collector) {
        return getCollectedInGroupsMapStream(students, collector).max(Comparator.comparingLong(Map.Entry::getValue))
                .orElse(new AbstractMap.SimpleEntry<>("", null)).getKey();
    }
}
