#ifndef UTIL_H
#define UTIL_H
struct list_head {
    struct list_head *next, *prev;
};

#define LIST_HEAD_INIT(name) { &(name), &(name) }
#define list_entry(ptr,type,member) container_of(ptr,type,member)
#define __offsetof(TYPE,MEMBER) ((size_t)&((TYPE *)0)->MEMBER)
#define container_of(ptr,type,member) ( {\
	const typeof( ((type*)0)->member ) *__mptr=(ptr);\
	(type*)( (char*)__mptr - __offsetof(type,member) );} )

#define list_for_each(pos, head) for(pos = (head)->next;pos != (head); pos = pos->next)

#define MAX_HASH 1024

static inline void INIT_LIST_HEAD(struct list_head *list){
    list->next = list;
    list->prev = list;
}

static inline void __list_add( struct list_head *new, struct list_head *prev, struct list_head *next){
    next->prev = new;
    new->next = next;
    new->prev = prev;
    prev->next = new;
}

static inline void list_add(struct list_head *new, struct list_head *head){
    __list_add(new, head, head->next);
}

static inline void list_add_tail(struct list_head *new, struct list_head *head){
    __list_add(new, head->prev, head);
}

static inline int list_empty(const struct list_head *head){
    return head->next == head;
}

static inline void __list_del(struct list_head * prev, struct list_head * next){
    next->prev = prev;
    prev->next = next;
}

static inline void list_del(struct list_head *entry){
    __list_del(entry->prev, entry->next);
    entry->next = entry;
    entry->prev = entry;
}


struct request_node{
    void* ptr;
    int index;
    struct list_head node;
};

struct list_head* init_request_pool(struct list_head* request_pool,int n){
    struct request_node* pool;
    int i;
    pool=(struct request_node*)malloc(n*sizeof(struct request_node));
    for(i=0;i<n;i++){
        pool[i].ptr=NULL;
        pool[i].index=-1;
        list_add(&(pool[i].node),request_pool); 
    }	 
    return request_pool;
}

struct request_node* pool_pop(struct list_head* pool_head){
    struct request_node* obj;
    if(list_empty(pool_head))
        return NULL;
    obj=list_entry(pool_head->next,struct request_node,node);
    list_del(&(obj->node));    
    return obj;
}

void pool_push(struct list_head* pool_head,struct request_node* obj){
    list_add_tail(&(obj->node),pool_head);
}

struct htable_node{
    struct list_head head;
};

void init_htable(struct htable_node* table){
    int i;
    for(i=0;i<MAX_HASH;i++){
      INIT_LIST_HEAD(&(table[i].head));
    }
}

static inline int cal_hash(void* ptr){
    return (long)ptr%MAX_HASH;
}

void hash_add_request(struct request_node* obj,struct htable_node* table){
    int idx=cal_hash(obj->ptr);
    struct list_head* head=&(table[idx].head);
    list_add_tail(&(obj->node),head);
}

struct request_node* hash_find_request(void* ptr,struct htable_node* table){
    int idx=cal_hash(ptr);
    struct list_head* head=&(table[idx].head);
    struct list_head* list;
    struct request_node* obj;
    list_for_each(list,head){
         obj=list_entry(list,struct request_node,node);
         if(obj->ptr==ptr){
             return obj;
         }
    }
    return NULL;
}

void recycle_request_node(struct request_node* obj,struct list_head* pool_head){
    list_del(&(obj->node));
    pool_push(pool_head,obj);
}

#endif
