package pkg

type Set[K comparable] struct {
	inner map[K]struct{}
}

// NewSet 创建一个新的空集合
func NewSet[K comparable]() *Set[K] {
	return &Set[K]{
		inner: make(map[K]struct{}),
	}
}

// NewSetWithValues 使用给定的值创建一个新的集合
func NewSetWithValues[K comparable](values ...K) *Set[K] {
	s := NewSet[K]()
	for _, v := range values {
		s.Add(v)
	}
	return s
}

// Add 向集合中添加一个元素
func (s *Set[K]) Add(key K) {
	s.inner[key] = struct{}{}
}

// Remove 从集合中移除一个元素
func (s *Set[K]) Remove(key K) {
	delete(s.inner, key)
}

// Contains 检查集合是否包含指定元素
func (s *Set[K]) Contains(key K) bool {
	_, exists := s.inner[key]
	return exists
}

// Size 返回集合中元素的数量
func (s Set[K]) Size() int {
	return len(s.inner)
}

// IsEmpty 检查集合是否为空
func (s *Set[K]) IsEmpty() bool {
	return len(s.inner) == 0
}

// Clear 清空集合中的所有元素
func (s *Set[K]) Clear() {
	s.inner = make(map[K]struct{})
}

// ToSlice 将集合转换为切片
func (s *Set[K]) ToSlice() []K {
	result := make([]K, 0, len(s.inner))
	for k := range s.inner {
		result = append(result, k)
	}
	return result
}

// Union 返回两个集合的并集
func (s *Set[K]) Union(other *Set[K]) *Set[K] {
	result := NewSet[K]()
	for k := range s.inner {
		result.Add(k)
	}
	for k := range other.inner {
		result.Add(k)
	}
	return result
}

// Intersection 返回两个集合的交集
func (s *Set[K]) Intersection(other *Set[K]) *Set[K] {
	result := NewSet[K]()
	for k := range s.inner {
		if other.Contains(k) {
			result.Add(k)
		}
	}
	return result
}

// Difference 返回两个集合的差集 (s - other)
func (s *Set[K]) Difference(other *Set[K]) *Set[K] {
	result := NewSet[K]()
	for k := range s.inner {
		if !other.Contains(k) {
			result.Add(k)
		}
	}
	return result
}

// IsSubsetOf 检查当前集合是否是另一个集合的子集
func (s *Set[K]) IsSubsetOf(other *Set[K]) bool {
	for k := range s.inner {
		if !other.Contains(k) {
			return false
		}
	}
	return true
}

// IsSupersetOf 检查当前集合是否是另一个集合的超集
func (s *Set[K]) IsSupersetOf(other *Set[K]) bool {
	return other.IsSubsetOf(s)
}

// Equal 检查两个集合是否相等
func (s *Set[K]) Equal(other *Set[K]) bool {
	if s.Size() != other.Size() {
		return false
	}
	return s.IsSubsetOf(other)
}

// Clone 创建集合的副本
func (s *Set[K]) Clone() *Set[K] {
	result := NewSet[K]()
	for k := range s.inner {
		result.Add(k)
	}
	return result
}

// ForEach 对集合中的每个元素执行给定的函数
func (s *Set[K]) ForEach(fn func(K)) {
	for k := range s.inner {
		fn(k)
	}
}

// Filter 返回一个新集合，包含满足条件的元素
func (s *Set[K]) Filter(predicate func(K) bool) *Set[K] {
	result := NewSet[K]()
	for k := range s.inner {
		if predicate(k) {
			result.Add(k)
		}
	}
	return result
}
